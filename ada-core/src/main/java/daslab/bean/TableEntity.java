package daslab.bean;

/**
 * @author zyz
 * @version 2018-07-27
 */
public class TableEntity {
    private String schema;
    private String table;

    public TableEntity(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String toSQL() {
        return String.format("%s.%s", schema, table);
    }

    @Override
    public String toString() {
        return toSQL();
    }
}
