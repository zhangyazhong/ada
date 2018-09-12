package daslab.bean;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zyz
 * @version 2018-05-18
 */
public class Sample {
    public String originalTable;
    public String sampleType;
    public String schemaName;
    public String tableName;
    public double samplingRatio;
    public String onColumn;
    public long tableSize;
    public long sampleSize;
    private Dataset<Row> rows;

    public Sample(String originalTable, String sampleType, String schemaName, String tableName, double samplingRatio, String onColumn, long tableSize, long sampleSize) {
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

    @Override
    public String toString() {
        return String.format("Sample - %s: %s.%s[%s] with %f on %s", tableName, schemaName, originalTable, sampleType, samplingRatio, onColumn);
    }

    public String brief() {
        return String.format("%s_%s", tableName, sampleType);
    }
}
