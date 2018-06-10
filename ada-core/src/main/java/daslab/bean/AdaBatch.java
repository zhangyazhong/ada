package daslab.bean;

/**
 * @author zyz
 * @version 2018-05-14
 */
public class AdaBatch {
    private String dbName;
    private String tableName;
    private int size;

    public AdaBatch(String dbName, String tableName, int size) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.size = size;
    }

    public static AdaBatch build(String dbName, String tableName, int size) {
        return new AdaBatch(dbName, tableName, size);
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public int getSize() {
        return size;
    }
}
