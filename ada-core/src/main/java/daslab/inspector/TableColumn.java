package daslab.inspector;

/**
 * @author zyz
 * @version 2018-05-14
 */
public class TableColumn {
    private int columnNo;
    private String columnName;
    private TableColumnType columnType;

    public TableColumn(int columnNo, String columnName, TableColumnType columnType) {
        this.columnNo = columnNo;
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public int getColumnNo() {
        return columnNo;
    }

    public String getColumnName() {
        return columnName;
    }

    public TableColumnType getColumnType() {
        return columnType;
    }

    @Override
    public String toString() {
        return String.format("[%d]%s: %s", columnNo, columnName, columnType);
    }
}
