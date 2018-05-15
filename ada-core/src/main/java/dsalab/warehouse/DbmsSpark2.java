package dsalab.warehouse;

import dsalab.context.AdaContext;
import dsalab.inspector.TableColumn;
import dsalab.inspector.TableColumnType;
import dsalab.inspector.TableSchema;
import dsalab.utils.AdaLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        sparkSession = SparkSession.builder().getOrCreate();
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
        df.foreach(row -> {
            int columnNo = index.get();
            String columnName = row.getString(0);
            TableColumnType columnType = TableColumnType.getType(row.getString(1));
            index.getAndIncrement();
            TableColumn column = new TableColumn(columnNo, columnName, columnType);
            tableSchema.addColumn(column);
        });
        return tableSchema;
    }

    public Dataset<Row> getResultSet() {
        return df;
    }

    public String getResultAsString(int rowNo, String col) {
        return df.collect()[rowNo].getString(df.schema().fieldIndex(col));
    }

    public int getResultAsInt(int rowNo, String col) {
        return df.collect()[rowNo].getInt(df.schema().fieldIndex(col));
    }

    public double getResultAsDouble(int rowNo, String col) {
        return df.collect()[rowNo].getDouble(df.schema().fieldIndex(col));
    }
}
