package daslab.sampling;

import com.google.common.collect.Lists;
import daslab.bean.AdaBatch;
import daslab.bean.Sample;
import daslab.context.AdaContext;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author zyz
 * @version 2018-05-15
 */
public abstract class SamplingStrategy {
    private AdaContext context;

    public SamplingStrategy(AdaContext context) {
        this.context = context;
    }

    public AdaContext getContext() {
        return context;
    }

    public List<Sample> getSamples() {
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
        List<Sample> samples = Lists.newArrayList();
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
        return samples;
    }

    public abstract void run(AdaBatch adaBatch);

    public abstract String name();
}
