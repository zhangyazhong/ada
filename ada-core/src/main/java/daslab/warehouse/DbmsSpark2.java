package daslab.warehouse;

import daslab.bean.Batch;
import daslab.context.AdaContext;
import daslab.inspector.TableColumn;
import daslab.inspector.TableColumnType;
import daslab.inspector.TableSchema;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zyz
 * @version 2018-05-12
 */
public class DbmsSpark2 {
    private static DbmsSpark2 dbmsSpark2;

    private AdaContext context;
    private SparkSession sparkSession;
    private Dataset<Row> df;

    static {
        dbmsSpark2 = null;
    }

    private DbmsSpark2(AdaContext context) {
        this.context = context;
        sparkSession = SparkSession
                .builder()
                .appName("Ada Core")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", context.get("dbms.warehouse.dir"))
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel(context.get("spark.log.level"));

        AdaLogger.info(this, "Available databases: " +
                StringUtils.join(execute("SHOW DATABASES").getResultSet().collectAsList().stream().map(row -> row.getString(0)).toArray(), ", "));

        execute(String.format("USE %s", context.get("dbms.default.database")));

        AdaLogger.info(this, String.format("Use %s as default database.", context.get("dbms.default.database")));
    }

    public static DbmsSpark2 getInstance(AdaContext context) {
        if (dbmsSpark2 != null) {
            return dbmsSpark2;
        }
        dbmsSpark2 = new DbmsSpark2(context);
        return dbmsSpark2;
    }

    public DbmsSpark2 execute(String sql) {
        AdaLogger.debug(this, "About to run: " + sql);
        df = sparkSession.sql(sql);
        return this;
    }

    public TableSchema desc() {
        return desc(context.get("dbms.data.table"));
    }

    public TableSchema desc(String tableName) {
        execute(String.format("DESC %s", tableName));
        AtomicInteger index = new AtomicInteger(0);
        TableSchema tableSchema = new TableSchema(context.get("dbms.default.database"), tableName);
        df.collectAsList().forEach(row -> {
            int columnNo = index.get();
            String columnName = row.getString(0);
            TableColumnType columnType = TableColumnType.getType(row.getString(1));
            index.getAndIncrement();
            TableColumn column = new TableColumn(columnNo, columnName, columnType);
            tableSchema.addColumn(column);
        });
        return tableSchema;
    }

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
        int size = (int) context.getDbmsSpark2().execute(query).getResultAsLong(0, "size");
        return new Batch(context.get("dbms.warehouse"), context.get("dbms.batch.table"), size);
    }

    public Dataset<Row> getResultSet() {
        return df;
    }

    public List<Row> getResultList() {
        return df.collectAsList();
    }

    public String getResultAsString(int rowNo, String col) {
        return df.collectAsList().get(rowNo).getString(df.schema().fieldIndex(col));
    }

    public int getResultAsInt(int rowNo, String col) {
        return df.collectAsList().get(rowNo).getInt(df.schema().fieldIndex(col));
    }

    public long getResultAsLong(int rowNo, String col) {
        return df.collectAsList().get(rowNo).getLong(df.schema().fieldIndex(col));
    }

    public double getResultAsDouble(int rowNo, String col) {
        return df.collectAsList().get(rowNo).getDouble(df.schema().fieldIndex(col));
    }
}
