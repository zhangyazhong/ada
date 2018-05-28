package daslab.bean;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zyz
 * @version 2018-05-18
 */
public class Sample {
    private String originalTable;
    private String sampleType;
    private String schemaName;
    private String tableName;
    private double samplingRatio;
    private String onColumn;
    private int tableSize;
    private int sampleSize;
    private Dataset<Row> rows;

    public Sample(String originalTable, String sampleType, String schemaName, String tableName, double samplingRatio, String onColumn, int tableSize, int sampleSize) {
        this.originalTable = originalTable;
        this.sampleType = sampleType;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.samplingRatio = samplingRatio;
        this.onColumn = onColumn;
        this.tableSize = tableSize;
        this.sampleSize = sampleSize;
    }

    public Dataset<Row> getRows() {
        return rows;
    }

    public void setRows(Dataset<Row> rows) {
        this.rows = rows;
    }
}
