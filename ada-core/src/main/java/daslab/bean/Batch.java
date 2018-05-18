package daslab.bean;

/**
 * @author zyz
 * @version 2018-05-14
 */
public class Batch {
    private String dbName;
    private String tableName;
    private int size;

    public Batch(String dbName, String tableName, int size) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.size = size;
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
