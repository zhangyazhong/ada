package dsalab.warehouse;

import dsalab.context.AdaContext;
import dsalab.utils.AdaLogger;

import java.io.File;
import java.sql.*;

/**
 * @author zyz
 * @version 2018-05-12
 */
public class DbmsHive2 {
    private static DbmsHive2 dbmsHive2;
    private AdaContext context;
    private static Connection connection;

    static {
        dbmsHive2 = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private DbmsHive2(AdaContext context) {
        this.context = context;
        try {
            String url = String.format("jdbc:%s://%s:%s/%s", context.get("dbms.warehouse"), context.get("dbms.host"),
                    context.get("dbms.port"), context.get("dbms.default.database"));
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static DbmsHive2 getInstance(AdaContext context) {
        if (dbmsHive2 != null) {
            return dbmsHive2;
        }
        dbmsHive2 = new DbmsHive2(context);
        return dbmsHive2;
    }

    public void execute(String sql) {
        try {
            AdaLogger.debug(this, "About to run: " + sql);

            connection.prepareStatement(sql).execute(sql);
        } catch (SQLException e){
            e.printStackTrace();
        }
    }

    public void load(String file) {
        load(new File(file));
    }

    public void load(File file) {
            String query = String.format("LOAD DATA LOCAL INPATH \"%s\" INTO TABLE %s",
                    file.getAbsolutePath(), context.get("dbms.data.table"));
            execute(query);

            AdaLogger.info(this, "Loaded batch into data table");

            query = String.format("TRUNCATE %s CASCADE", context.get("dbms.batch.table"));
            execute(query);

            query = String.format("LOAD DATA LOCAL INPATH \"%s\" INTO TABLE %s",
                    file.getAbsolutePath(), context.get("dbms.batch.table"));
            execute(query);

            AdaLogger.info(this, "Loaded batch into batch table");
    }
}
