package daslab.sampling;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.bean.*;
import daslab.context.AdaContext;
import daslab.inspector.TableColumn;
import daslab.inspector.TableColumnSample;
import daslab.inspector.TableMeta;
import daslab.utils.AdaLogger;
import org.apache.spark.sql.Row;

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
        String sampleDb = context.get("dbms.sample.database");
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
        List<Row> rows = context.getDbmsSpark2().execute(sql).getResultList();
        samples = Lists.newArrayList();
        for (Row row: rows) {
            if (row.getString(0).equals(context.get("dbms.data.table"))
                    && row.getString(1).equals("uniform")) {
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
        samples.forEach(sample -> AdaLogger.info(this, sample.toString()));
        if (samples.size() == 0) {
            AdaLogger.info(this, "Sample - no sample available");
        }
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
        List<Sample> samples = getSamples(false);
        for (Sample sample : samples) {
            AdaLogger.info(this, String.format("Sample[%.2f] size is: %d", sample.samplingRatio, sample.sampleSize));
            SampleStatus sampleStatus = new SampleStatus(sample, tableSize);
            for (TableColumn column : metaMap.keySet()) {
                sampleStatus.push(column, (long) metaMap.get(column).getN());
            }
        }
        return sampleStatusMap;
    }

    public abstract void run(Sample sample, AdaBatch adaBatch);

    public abstract void update(Sample sample, AdaBatch adaBatch);

    public abstract void resample(Sample sample, AdaBatch adaBatch, double ratio);

    public abstract String name();
}
