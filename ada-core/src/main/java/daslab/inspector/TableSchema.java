package daslab.inspector;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @author zyz
 * @version 2018-05-14
 */
public class TableSchema {
    private String dbName;
    private String tableName;
    private List<TableColumn> columns;

    public TableSchema(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.columns = Lists.newArrayList();
    }

    public void addColumn(TableColumn column) {
        columns.add(column);
    }

    public TableColumn getColumnByNo(int no) {
        for (TableColumn column : columns) {
            if (column.getColumnNo() == no) {
                return column;
            }
        }
        return null;
    }

    public TableColumn getColumnByName(String name) {
        for (TableColumn column : columns) {
            if (column.getColumnName().equals(name)) {
                return column;
            }
        }
        return null;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public List<TableColumn> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        return String.format("%s: {%s}", tableName,
                StringUtils.join(this.columns.stream().map(TableColumn::toString).toArray(), ","));
    }
}
