package daslab.sampling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import daslab.bean.*;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;
import daslab.utils.AdaNamespace;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static java.lang.Math.round;
/**
 * @author zyz
 * @version 2018-06-05
 */
@SuppressWarnings("Duplicates")
public class VerdictSampling extends SamplingStrategy {
    private VerdictSpark2Context verdictSpark2Context;

    public VerdictSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(Sample sample, AdaBatch adaBatch) {
    }

    @Override
    public void update(Sample sample, AdaBatch adaBatch) {
    }

    @Override
    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
        ratio = (1.0 * (int) round(ratio * 100)) / 100;
        verdictSpark2Context = getContext().getVerdict();
        AdaLogger.info(this, "About to drop sample with ratio " + sample.samplingRatio + " of " + sample.sampleType);
        deleteSampleMeta(sample);
        deleteSampleTable(sample);
        createSample(sample, ratio);
        refreshMetaSize();
    }

    private void deleteSampleTable(Sample sample) {
        String sampleSchema = sample.schemaName;
        String sampleTable = sample.tableName;
        getContext().getDbmsSpark2().execute(String.format("DROP TABLE IF EXISTS %s.%s", sampleSchema, sampleTable));
    }

    private void deleteSampleMeta(Sample sample) {
        SparkSession spark = getContext().getDbmsSpark2().getSparkSession();
        List<Sample> samples = getSamples(true);
        List<Dataset<Row>> metaSizeDFs = Lists.newArrayList();
        List<Dataset<Row>> metaNameDFs = Lists.newArrayList();
        for (Sample _sample : samples) {
            Dataset<Row> metaSizeDF;
            Dataset<Row> metaNameDF;
            if (Math.abs(_sample.samplingRatio - sample.samplingRatio) > 0.00001 || !_sample.sampleType.equals(sample.sampleType) || !_sample.onColumn.equals(sample.onColumn)) {
                metaSizeDF = spark
                        .createDataFrame(ImmutableList.of(new VerdictMetaSize(_sample.schemaName, _sample.tableName, _sample.sampleSize, _sample.tableSize)), VerdictMetaSize.class)
                        .toDF();
                metaNameDF = spark
                        .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), _sample.originalTable, _sample.schemaName, _sample.tableName, _sample.sampleType, _sample.samplingRatio, _sample.onColumn)), VerdictMetaName.class)
                        .toDF();
                metaSizeDFs.add(metaSizeDF);
                metaNameDFs.add(metaNameDF);
            }
        }
        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(String.format("DROP TABLE IF EXISTS  %s.%s", sample.schemaName, "verdict_meta_name"))
                .execute(String.format("DROP TABLE IF EXISTS %s.%s", sample.schemaName, "verdict_meta_size"));
        if (metaNameDFs.size() > 0) {
            Dataset<Row> metaSizeDF = metaSizeDFs.get(0);
            Dataset<Row> metaNameDF = metaNameDFs.get(0);
            for (int i = 1; i < metaNameDFs.size(); i++) {
                metaSizeDF = metaSizeDF.union(metaSizeDFs.get(i));
                metaNameDF = metaNameDF.union(metaNameDFs.get(i));
            }
            metaNameDF.select("originalschemaname", "originaltablename", "sampleschemaaname", "sampletablename", "sampletype", "samplingratio", "columnnames").write().saveAsTable("verdict_meta_name");
            metaSizeDF.select("schemaname", "tablename", "samplesize", "originaltablesize").write().saveAsTable("verdict_meta_size");
        }
    }

    private void refreshMetaSize() {
        SparkSession spark = getContext().getDbmsSpark2().getSparkSession();
        long cardinality = Long.parseLong(getContext().get(getContext().get("dbms.data.table") + "_cardinality"));
        List<Row> metaSizes = spark.sql(String.format("SELECT * FROM %s.%s", getContext().get("dbms.sample.database"), "verdict_meta_size")).collectAsList();
        List<Dataset<Row>> metaSizeDFs = Lists.newArrayList();
        for (Row row : metaSizes) {
            long tableSize;
            if (row.getString(row.fieldIndex("tablename")).contains("vs_" + getContext().get("dbms.data.table") + "_")) {
                tableSize = cardinality;
            } else {
                tableSize = row.getLong(row.fieldIndex("originaltablesize"));
            }
            Dataset<Row> metaSizeDF = spark
                    .createDataFrame(ImmutableList.of(new VerdictMetaSize(row.getString(row.fieldIndex("schemaname")), row.getString(row.fieldIndex("tablename")), row.getLong(row.fieldIndex("samplesize")), tableSize)), VerdictMetaSize.class)
                    .toDF();
            metaSizeDFs.add(metaSizeDF);
        }
        if (metaSizeDFs.size() > 0) {
            getContext().getDbmsSpark2()
                    .execute(String.format("USE %s", getContext().get("dbms.sample.database")))
                    .execute(String.format("DROP TABLE IF EXISTS %s.%s", getContext().get("dbms.sample.database"), "verdict_meta_size"));
            Dataset<Row> metaSizeDF = metaSizeDFs.get(0);
            for (int i = 1; i < metaSizeDFs.size(); i++) {
                metaSizeDF = metaSizeDF.union(metaSizeDFs.get(i));
            }
            metaSizeDF.select("schemaname", "tablename", "samplesize", "originaltablesize").write().saveAsTable("verdict_meta_size");
        }
    }

    private void createSample(Sample sample, double ratio) {
        switch (sample.sampleType) {
            case "uniform":
                createUniformSample(sample, ratio);
                break;
            case "stratified":
                createStratifiedSample(sample, ratio);
                break;
        }
    }

    private void createUniformSample(Sample sample, double ratio) {
        AdaLogger.info(this, String.format("About to create uniform sample with sampling ratio %f of %s.%s", round(ratio * 100) / 100.0, getContext().get("dbms.default.database"), sample.originalTable));
        try {
            verdictSpark2Context.sql("CREATE " + (int) round(ratio * 100) + "% UNIFORM SAMPLE OF " + getContext().get("dbms.default.database") + "." + sample.originalTable);
        } catch (VerdictException e) {
            e.printStackTrace();
        }
    }

    private void createStratifiedSample(Sample sample, double ratio) {
        AdaLogger.info(this, String.format("About to create stratified sample with sampling ratio %f of %s.%s", round(ratio * 100) / 100.0, getContext().get("dbms.default.database"), getContext().get("dbms.data.table")));
        TableEntity originTable = new TableEntity(getContext().get("dbms.default.database"), sample.originalTable);
        TableEntity groupTable = createGroupTable(originTable, sample.onColumn);
        TableEntity stratifiedSampleWithoutProb = createStratifiedSampleWithoutProb(originTable, groupTable, sample.onColumn, ratio);
        TableEntity stratifiedSampleWithProb = attachProbToStratifiedSample(originTable, stratifiedSampleWithoutProb, sample.onColumn, ratio);
        dropGroupTable(sample, groupTable);
        dropStratifiedSampleWithoutProb(stratifiedSampleWithoutProb);
        long sampleCardinality = getContext().getDbms().count(stratifiedSampleWithProb);
        insertMetaInfo(sample,
                getContext().getDbms().getSparkSession()
                        .createDataFrame(ImmutableList.of(new VerdictMetaSize(stratifiedSampleWithProb.getSchema(), stratifiedSampleWithProb.getTable(), sampleCardinality, getContext().getTableMeta().getCardinality())), VerdictMetaSize.class).toDF(),
                getContext().getDbms().getSparkSession()
                        .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, stratifiedSampleWithProb.getSchema(), stratifiedSampleWithProb.getTable(), sample.sampleType, ratio, sample.onColumn)), VerdictMetaName.class).toDF()
        );
        AdaLogger.info(this, String.format("Created stratified sample stored in %s", stratifiedSampleWithProb.toSQL()));
    }

    /**
     * create group size for specified table on one column
     * @param originTable specified table
     * @return group table e.g. `wiki.pagecounts` on `project_name` -> {`project_name`, `verdict_group_size`}
     */
    private TableEntity createGroupTable(TableEntity originTable, String onColumn) {
        TableEntity groupTable = new TableEntity(getContext().get("dbms.verdict.database"), AdaNamespace.tempUniqueName("verdict_group"));
        String sql = String.format("CREATE TABLE %s AS SELECT %s AS %s, count(*) AS verdict_group_size FROM %s GROUP BY %s",
                groupTable.toSQL(), onColumn, onColumn, originTable.toSQL(), onColumn);
        getContext().getDbms().execute(sql);
        return groupTable;
    }

    private TableEntity createStratifiedSampleWithoutProb(TableEntity originTable, TableEntity groupTable, String onColumn, double ratio) {
        List<Long> groupInfoList = getContext().getDbms().execute("SELECT * FROM " + groupTable.toSQL()).getResultSet()
                .map((MapFunction<Row, Long>) row -> row.getLong(row.fieldIndex("verdict_group_size")),
                Encoders.LONG()).collectAsList();
        long totalCardinality = 0;
        for (Long count : groupInfoList) {
            totalCardinality += count;
        }
        long sampleCardinality = (long) (totalCardinality * ratio);
        long eachGroupSampleCardinalityMin = 0, eachGroupSampleCardinalityMax = sampleCardinality;
        long eachGroupSampleCardinality = (eachGroupSampleCardinalityMin + eachGroupSampleCardinalityMax) >> 1; //= Math.max(10, sampleCardinality / groupCount + 1);
        while (eachGroupSampleCardinalityMin <= eachGroupSampleCardinalityMax) {
            eachGroupSampleCardinality = (eachGroupSampleCardinalityMin + eachGroupSampleCardinalityMax) >> 1;
            long expectedSampleCardinality = 0;
            for (Long count : groupInfoList) {
                expectedSampleCardinality += Math.min(count, eachGroupSampleCardinality);
            }
            if (expectedSampleCardinality < sampleCardinality * 0.95) {
                eachGroupSampleCardinalityMin = eachGroupSampleCardinality + 1;
            } else if (expectedSampleCardinality > sampleCardinality * 1.05) {
                eachGroupSampleCardinalityMax = eachGroupSampleCardinality - 1;
            } else {
                break;
            }
        }
        TableEntity stratifiedSampleTable = new TableEntity(groupTable.getSchema(), AdaNamespace.tempUniqueName("verdict_stratified"));
        String sql = String.format("CREATE TABLE %s AS " +
                "SELECT s.*, `verdict_group_size` AS `verdict_group_size` " +
                "FROM (SELECT *, Rand(Unix_timestamp()) AS `verdict_rand` FROM %s) AS s " +
                "INNER JOIN %s AS t ON ( CASE WHEN ( s.%s IS NULL ) THEN 'VERDICT_NULL' ELSE s.%s END ) = ( CASE WHEN ( t.%s IS NULL ) THEN 'VERDICT_NULL' ELSE t.%s END ) " +
                "WHERE ( `verdict_rand` < ( 1.0 * %d / `verdict_group_size` ) ) " +
                    "OR ( `verdict_rand` < ( CASE " +
                        "WHEN `verdict_group_size` >= 100 THEN ( ( 0.203759 * 100 ) / `verdict_group_size` ) " +
                        "WHEN `verdict_group_size` >= 50 THEN ( ( 0.376508 * 50 ) / `verdict_group_size` ) " +
                        "WHEN `verdict_group_size` >= 40 THEN ( ( 0.452739 * 40 ) / `verdict_group_size` ) " +
                        "WHEN `verdict_group_size` >= 30 THEN ( ( 0.566406 * 30 ) / `verdict_group_size` ) " +
                        "WHEN `verdict_group_size` >= 20 THEN ( ( 0.749565 * 20 ) / `verdict_group_size` ) " +
                        "WHEN `verdict_group_size` >= 15 THEN ( ( 0.881575 * 15 ) / `verdict_group_size` ) " +
                        "WHEN `verdict_group_size` >= 14 THEN ( ( 0.910660 * 14 ) / `verdict_group_size` ) " +
                        "WHEN `verdict_group_size` >= 13 THEN ( ( 0.939528 * 13 ) / `verdict_group_size` ) " +
                        "WHEN `verdict_group_size` >= 12 THEN ( ( 0.966718 * 12 ) / `verdict_group_size` ) " +
                        "WHEN `verdict_group_size` >= 11 THEN ( ( 0.989236 * 11 ) / `verdict_group_size` ) " +
                    "ELSE 1.0 END ) ) ",
                stratifiedSampleTable.toSQL(), originTable.toSQL(), groupTable.toSQL(),
                onColumn, onColumn, onColumn, onColumn,
                eachGroupSampleCardinality);
        getContext().getDbms().execute(sql);
        return stratifiedSampleTable;
    }

    private TableEntity attachProbToStratifiedSample(TableEntity originTable, TableEntity stratifiedSampleWithoutProb, String onColumn, double ratio) {
        TableEntity stratifiedSampleWithProb = new TableEntity(stratifiedSampleWithoutProb.getSchema(),
                String.format("vs_%s_st_0_%d_%s", originTable.getTable(), ((int) Math.round(ratio * 100)) * 100, onColumn));
        String sql = String.format("CREATE TABLE %s stored AS parquet AS " +
                "SELECT s.*, (`verdict_group_size_in_sample` / `verdict_group_size`) AS `verdict_vprob`, (round((rand(unix_timestamp()) * 100)) %% 100) AS `verdict_vpart` " +
                "FROM %s AS s " +
                "INNER JOIN " +
                    "( SELECT   %s AS %s, count(*) AS `verdict_group_size_in_sample` " +
                    "FROM %s " +
                    "GROUP BY %s ) AS t " +
                "ON ( CASE WHEN ( s.%s IS NULL) THEN 'VERDICT_NULL' ELSE s.%s END ) = ( CASE WHEN ( t.%s IS NULL) THEN 'VERDICT_NULL' ELSE t.%s END)",
                stratifiedSampleWithProb.toSQL(), stratifiedSampleWithoutProb.toSQL(),
                onColumn, onColumn, stratifiedSampleWithoutProb.toSQL(), onColumn,
                onColumn, onColumn, onColumn, onColumn);
        getContext().getDbms().execute(sql);
        return stratifiedSampleWithProb;
    }

    private void dropGroupTable(Sample sample, TableEntity groupTable) {
        TableEntity oldGroupTable = new TableEntity(sample.schemaName, String.format("ada_%s_group_%s", sample.originalTable, sample.onColumn));
        getContext().getDbms().drop(oldGroupTable);
        getContext().getDbms()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(String.format("CREATE TABLE %s AS (SELECT verdict_group_size AS group_size, %s FROM %s)", oldGroupTable.getTable(), sample.onColumn, groupTable.toSQL()))
                .drop(groupTable);
    }

    private void dropStratifiedSampleWithoutProb(TableEntity stratifiedSampleWithoutProb) {
        getContext().getDbms().drop(stratifiedSampleWithoutProb);
    }

    @Override
    public String name() {
        return "verdict sampling";
    }
}
