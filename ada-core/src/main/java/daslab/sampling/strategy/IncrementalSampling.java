package daslab.sampling.strategy;

import com.google.common.collect.ImmutableList;
import daslab.bean.*;
import daslab.context.AdaContext;
import daslab.sampling.SamplingStrategy;
import daslab.utils.AdaLogger;
import daslab.utils.AdaNamespace;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Random;

import static java.lang.Math.round;
import static org.apache.spark.sql.functions.lit;

/**
 * @author zyz
 * @version 2018-06-05
 */
@SuppressWarnings("Duplicates")
public class IncrementalSampling extends SamplingStrategy {
    public IncrementalSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(Sample sample, AdaBatch adaBatch) {
    }

    @Override
    public void update(Sample sample, AdaBatch adaBatch) {
        switch (sample.sampleType) {
            case "uniform":
                updateUniformSample(sample, adaBatch);
                break;
            case "stratified":
                updateStratifiedSample(sample, adaBatch);
                break;
        }
    }

    @Override
    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
        update(sample, adaBatch);
    }

    private void updateUniformSample(Sample sample, AdaBatch adaBatch) {
        Random randomGenerator = new Random();
        long cardinality = getContext().getTableMeta().getCardinality();
        Dataset<Row> batchSampleDF = getContext().getDbmsSpark2()
                .execute(String.format("SELECT * FROM %s.%s", adaBatch.getDbName(), adaBatch.getTableName()))
                .getResultSet()
                .sample(false, sample.samplingRatio)
                .withColumn("verdict_rand", lit(randomGenerator.nextDouble() * sample.sampleSize / cardinality))
                .withColumn("verdict_vpart", lit(Math.floor(randomGenerator.nextDouble() * 100)));
        Dataset<Row> originSampleDF = getContext().getDbmsSpark2()
                .execute(String.format("SELECT * FROM %s.%s", sample.schemaName, sample.tableName))
                .getResultSet();
        long totalSize = originSampleDF.count() + batchSampleDF.count();
        Dataset<Row> updatedSample = batchSampleDF.union(originSampleDF);

        updateMetaInfo(sample,
                getContext().getDbms().getSparkSession()
                        .createDataFrame(ImmutableList.of(new VerdictMetaSize(sample.schemaName, sample.tableName, totalSize, cardinality)), VerdictMetaSize.class).toDF(),
                getContext().getDbms().getSparkSession()
                        .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, sample.samplingRatio, sample.onColumn)), VerdictMetaName.class).toDF()
        );
        updatedSample.write().saveAsTable(sample.tableName + "_tmp");

        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(String.format("DROP TABLE IF EXISTS %s.%s", sample.schemaName, sample.tableName))
                .execute(String.format("ALTER TABLE %s_tmp RENAME TO %s", sample.tableName, sample.tableName));
    }

    private void updateStratifiedSample(Sample sample, AdaBatch adaBatch) {
        double ratio = Math.max(1.0 * (int) round(sample.samplingRatio * 100) / 100, 0.01);
        long cardinality = getContext().getTableMeta().getCardinality();
        TableEntity batchTable = new TableEntity(adaBatch.getDbName(), adaBatch.getTableName());
        TableEntity groupTable = createGroupTable(batchTable, sample.onColumn);
        TableEntity stratifiedSampleWithoutProb = createStratifiedSampleWithoutProb(batchTable, groupTable, sample.onColumn, ratio);
        TableEntity stratifiedSampleWithProb = attachProbToStratifiedSample(batchTable, stratifiedSampleWithoutProb, sample.onColumn, ratio);
        String mergeSQL = String.format("INSERT INTO %s.%s SELECT * FROM %s", sample.schemaName, sample.tableName, stratifiedSampleWithProb.toSQL());
        getContext().getDbms().execute(mergeSQL);
        dropGroupTable(sample, groupTable);
        dropStratifiedSampleWithoutProb(stratifiedSampleWithoutProb);
        long sampleCardinality = sample.sampleSize + getContext().getDbms().count(stratifiedSampleWithProb);
        updateMetaInfo(sample,
                getContext().getDbms().getSparkSession()
                        .createDataFrame(ImmutableList.of(new VerdictMetaSize(sample.schemaName, sample.tableName, sampleCardinality, cardinality)), VerdictMetaSize.class).toDF(),
                getContext().getDbms().getSparkSession()
                        .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, sample.samplingRatio, sample.onColumn)), VerdictMetaName.class).toDF()
        );
        AdaLogger.info(this, String.format("Created stratified sample stored in %s", stratifiedSampleWithProb.toSQL()));
    }

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
        return "incremental";
    }

    @Override
    public String nameInPaper() {
        return "AS";
    }
}
