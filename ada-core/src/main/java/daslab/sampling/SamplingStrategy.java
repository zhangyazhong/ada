package daslab.sampling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.bean.*;
import daslab.context.AdaContext;
import daslab.inspector.TableColumn;
import daslab.inspector.TableMeta;
import daslab.utils.AdaLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

/**
 * @author zyz
 * @version 2018-05-15
 */
public abstract class SamplingStrategy {
    private AdaContext context;
    private List<Sample> samples;

    public SamplingStrategy(AdaContext context) {
        this.context = context;
    }

    public AdaContext getContext() {
        return context;
    }

    public List<Sample> getSamples(boolean refresh) {
        if (!refresh) {
            return samples;
        }
        samples = Lists.newArrayList();
        String sampleDb = context.get("dbms.sample.database");
        List<Row> rows = context.getDbmsSpark2().execute(String.format("SHOW TABLES FROM %s", sampleDb)).getResultList();
        boolean metaNameExists = false;
        boolean metaSizeExists = false;
        for (Row row : rows) {
            metaNameExists = row.getString(row.fieldIndex("tableName")).equals("verdict_meta_name") || metaNameExists;
            metaSizeExists = row.getString(row.fieldIndex("tableName")).equals("verdict_meta_size") || metaSizeExists;
        }
        if (!metaNameExists || !metaSizeExists) {
            return samples;
        }
        String sql = String.format(
                "SELECT s.`originaltablename` AS `original_table`, "
                        + "s.`sampletype` AS `sample_type`, "
                        + "t.`schemaname` AS `sample_schema_name`, "
                        + "s.`sampletablename` AS `sample_table_name`, "
                        + "s.`samplingratio` AS `sampling_ratio`, "
                        + "s.`columnnames` AS `on_columns`, "
                        + "t.`originaltablesize` AS `original_table_size`, "
                        + "t.`samplesize` AS `sample_table_size` "
                        + "FROM %s.verdict_meta_name AS s "
                        + "INNER JOIN %s.verdict_meta_size AS t "
                        + "ON s.`sampleschemaaname` = t.`schemaname` AND s.`sampletablename` = t.`tablename` "
                        + "ORDER BY `original_table`, `sample_type`, `sampling_ratio`, `on_columns`", sampleDb, sampleDb);
        rows = context.getDbmsSpark2().execute(sql).getResultList();
        for (Row row: rows) {
            if (row.getString(0).equals(context.get("dbms.data.table"))) {
//                    && row.getString(1).equals("uniform")) {
                Sample sample = new Sample(row.getString(row.schema().fieldIndex("original_table")),
                        row.getString(row.schema().fieldIndex("sample_type")),
                        row.getString(row.schema().fieldIndex("sample_schema_name")),
                        row.getString(row.schema().fieldIndex("sample_table_name")),
                        row.getDouble(row.schema().fieldIndex("sampling_ratio")),
                        row.getString(row.schema().fieldIndex("on_columns")),
                        row.getLong(row.schema().fieldIndex("original_table_size")),
                        row.getLong(row.schema().fieldIndex("sample_table_size")));
                samples.add(sample);
            }
        }
        /*
        samples.forEach(sample -> AdaLogger.info(this, sample.toString()));
        if (samples.size() == 0) {
            AdaLogger.info(this, "Sample - no sample available");
        }
        */
        return samples;
    }

    public List<VerdictMetaName> getMetaNames() {
        String sampleDb = context.get("dbms.sample.database");
        String sql = String.format("SELECT * FROM %s.verdict_meta_name", sampleDb);
        List<Row> metaNameRows = context.getDbmsSpark2().execute(sql).getResultList();
        List<VerdictMetaName> metaNameList = Lists.newArrayList();
        for (Row row : metaNameRows) {
            metaNameList.add(new VerdictMetaName(
                    row.getString(row.fieldIndex("originalschemaname")),
                    row.getString(row.fieldIndex("originaltablename")),
                    row.getString(row.fieldIndex("sampleschemaaname")),
                    row.getString(row.fieldIndex("sampletablename")),
                    row.getString(row.fieldIndex("sampletype")),
                    row.getDouble(row.fieldIndex("samplingratio")),
                    row.getString(row.fieldIndex("columnnames"))
            ));
        }
        return metaNameList;
    }

    public List<VerdictMetaSize> getMetaSizes() {
        String sampleDb = context.get("dbms.sample.database");
        String sql = String.format("SELECT * FROM %s.verdict_meta_size", sampleDb);
        List<Row> metaSizeRows = context.getDbmsSpark2().execute(sql).getResultList();
        List<VerdictMetaSize> metaSizeList = Lists.newArrayList();
        for (Row row : metaSizeRows) {
            metaSizeList.add(new VerdictMetaSize(
                    row.getString(row.fieldIndex("schemaname")),
                    row.getString(row.fieldIndex("tablename")),
                    row.getLong(row.fieldIndex("samplesize")),
                    row.getLong(row.fieldIndex("originaltablesize"))
            ));
        }
        return metaSizeList;
    }

    public Map<Sample, SampleStatus> verify(Map<TableColumn, TableMeta.MetaInfo> metaMap, long tableSize) {
        Map<Sample, SampleStatus> sampleStatusMap = Maps.newHashMap();
        List<Sample> samples = getSamples(true);
        for (Sample sample : samples) {
            AdaLogger.info(this, String.format("Sample[%s][%.2f] size is: %d", sample.sampleType, sample.samplingRatio, sample.sampleSize));
            SampleStatus sampleStatus = new SampleStatus(sample, tableSize);
            for (TableColumn column : metaMap.keySet()) {
                sampleStatus.push(column, (long) metaMap.get(column).getN());
            }
            sampleStatusMap.put(sample, sampleStatus);
        }
        return sampleStatusMap;
    }

    public void insertMetaInfo(Sample sample, Dataset<Row> metaSizeRow, Dataset<Row> metaNameRow) {
        SparkSession spark = getContext().getDbms().getSparkSession();
        List<Sample> samples = getSamples(true);
        List<Dataset<Row>> metaSizeDFs = Lists.newArrayList();
        List<Dataset<Row>> metaNameDFs = Lists.newArrayList();
        for (Sample _sample : samples) {
            Dataset<Row> metaSizeDF;
            Dataset<Row> metaNameDF;
            metaSizeDF = spark
                    .createDataFrame(ImmutableList.of(new VerdictMetaSize(_sample.schemaName, _sample.tableName, _sample.sampleSize, _sample.tableSize)), VerdictMetaSize.class)
                    .toDF();
            metaNameDF = spark
                    .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), _sample.originalTable, _sample.schemaName, _sample.tableName, _sample.sampleType, _sample.samplingRatio, _sample.onColumn)), VerdictMetaName.class)
                    .toDF();
            metaSizeDFs.add(metaSizeDF);
            metaNameDFs.add(metaNameDF);
        }
        Dataset<Row> metaSizeDF = metaSizeRow;
        Dataset<Row> metaNameDF = metaNameRow;
        for (int i = 0; i < metaNameDFs.size(); i++) {
            metaSizeDF = metaSizeDF.union(metaSizeDFs.get(i));
            metaNameDF = metaNameDF.union(metaNameDFs.get(i));
        }
        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(String.format("DROP TABLE IF EXISTS %s.%s", sample.schemaName, "verdict_meta_name"))
                .execute(String.format("DROP TABLE IF EXISTS %s.%s", sample.schemaName, "verdict_meta_size"));
        metaNameDF.select("originalschemaname", "originaltablename", "sampleschemaaname", "sampletablename", "sampletype", "samplingratio", "columnnames").write().saveAsTable("verdict_meta_name");
        metaSizeDF.select("schemaname", "tablename", "samplesize", "originaltablesize").write().saveAsTable("verdict_meta_size");
    }

    public void updateMetaInfo(Sample sample, Dataset<Row> metaSizeRow, Dataset<Row> metaNameRow) {
        SparkSession spark = getContext().getDbms().getSparkSession();
        List<Sample> samples = getSamples(true);
        List<Dataset<Row>> metaSizeDFs = Lists.newArrayList();
        List<Dataset<Row>> metaNameDFs = Lists.newArrayList();
        for (Sample _sample : samples) {
            Dataset<Row> metaSizeDF;
            Dataset<Row> metaNameDF;
            if (Math.abs(_sample.samplingRatio - sample.samplingRatio) < 0.00001 && _sample.sampleType.equals(sample.sampleType) && _sample.onColumn.equals(sample.onColumn)) {
                continue;
            } else {
                metaSizeDF = spark
                        .createDataFrame(ImmutableList.of(new VerdictMetaSize(_sample.schemaName, _sample.tableName, _sample.sampleSize, _sample.tableSize)), VerdictMetaSize.class)
                        .toDF();
                metaNameDF = spark
                        .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), _sample.originalTable, _sample.schemaName, _sample.tableName, _sample.sampleType, _sample.samplingRatio, _sample.onColumn)), VerdictMetaName.class)
                        .toDF();
            }
            metaSizeDFs.add(metaSizeDF);
            metaNameDFs.add(metaNameDF);
        }
        Dataset<Row> metaSizeDF = metaSizeRow;
        Dataset<Row> metaNameDF = metaNameRow;
        for (int i = 0; i < metaNameDFs.size(); i++) {
            metaSizeDF = metaSizeDF.union(metaSizeDFs.get(i));
            metaNameDF = metaNameDF.union(metaNameDFs.get(i));
        }
        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(String.format("DROP TABLE IF EXISTS %s.%s", sample.schemaName, "verdict_meta_name"))
                .execute(String.format("DROP TABLE IF EXISTS %s.%s", sample.schemaName, "verdict_meta_size"));
        metaNameDF.select("originalschemaname", "originaltablename", "sampleschemaaname", "sampletablename", "sampletype", "samplingratio", "columnnames").write().saveAsTable("verdict_meta_name");
        metaSizeDF.select("schemaname", "tablename", "samplesize", "originaltablesize").write().saveAsTable("verdict_meta_size");
    }

    public abstract void run(Sample sample, AdaBatch adaBatch);

    public abstract void update(Sample sample, AdaBatch adaBatch);

    public abstract void resample(Sample sample, AdaBatch adaBatch, double ratio);

    public abstract String name();

    public abstract String nameInPaper();
}
