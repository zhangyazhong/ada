package daslab.warehouse;

import daslab.bean.Batch;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;

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
            connection = DriverManager.getConnection(url, "hadoop", "");
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

    /*
    public Batch load(String file) {
        return load(new File(file));
    }

    public Batch load(File file) {
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

            query = String.format("SELECT count(*) AS size FROM %s", context.get("dbms.batch.table"));
            // OPTIMIZE: hive connector but invoke spark method.
            int size = context.getDbmsSpark2().execute(query).getResultAsInt(0, "size");
            return new Batch(context.get("dbms.warehouse"), context.get("dbms.batch.table"), size);
    }
    */
}
