package daslab.sampling.strategy;

import com.google.common.collect.ImmutableList;
import daslab.bean.*;
import daslab.context.AdaContext;
import daslab.sampling.SamplingStrategy;
import daslab.utils.AdaLogger;
import daslab.utils.AdaNamespace;
import daslab.utils.AdaTimer;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Random;

import static org.apache.spark.sql.functions.lit;

@SuppressWarnings("Duplicates")
public class AdaptiveSampling extends SamplingStrategy {
    ;

    public AdaptiveSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(Sample sample, AdaBatch adaBatch) {
    }

    @Override
    public void update(Sample sample, AdaBatch adaBatch) {
        switch (sample.sampleType) {
            case "stratified":
                updateStratified(sample, adaBatch);
                break;
            case "uniform":
                updateUniform(sample, adaBatch);
                break;
        }
    }

    @Override
    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
        update(sample, adaBatch);
    }

    private void updateUniform(Sample sample, AdaBatch adaBatch) {
        Random random = new Random();
        long x = findX(sample, adaBatch);
        // REPORT: adaptive.X
        getContext().writeIntoReport("adaptive.X", x);
        TableEntity sampleTable = new TableEntity(sample.schemaName, sample.tableName);
        TableEntity batchTable = new TableEntity(adaBatch.getDbName(), adaBatch.getTableName());

        // REPORT: sampling.cost.clean (start)
        AdaTimer timer = AdaTimer.create();
        String sqlForClean = String.format("SELECT * FROM %s WHERE Rand(Unix_timestamp())<%f",
                sampleTable.toSQL(), 1.0 * x / sample.sampleSize);
        Dataset<Row> cleanedSample = getContext().getDbmsSpark2()
                .execute(sqlForClean)
                .getResultSet();
        long cleanedCount = cleanedSample.count();
        AdaLogger.info(this, "Sample cleaned row count: " + cleanedCount);
        // REPORT: sampling.cost.clean (stop)
        getContext().writeIntoReport("sampling.cost.clean", timer.stop());
        // REPORT: sampling.count.clean
        getContext().writeIntoReport("sampling.count.clean", cleanedCount);

        double vpart = Math.floor(random.nextDouble() * 100);
        double rand = random.nextDouble() * sample.sampleSize / sample.tableSize;

        // REPORT: sampling.cost.insert (start)
        timer = AdaTimer.create();
        Dataset<Row> insertedSample = getContext().getDbmsSpark2()
                .execute(String.format("SELECT * FROM %s WHERE Rand(Unix_timestamp())<%f", batchTable.toSQL(), 1.0 * (getContext().getSampleStatus(sample).getMaxExpectedSize() - x) / adaBatch.getSize()))
                .getResultSet()
                .withColumn("verdict_rand", lit(rand))
                .withColumn("verdict_vpart", lit(vpart));
        long insertedCount = insertedSample.count();
        AdaLogger.info(this, "Sample inserted row count: " + insertedCount);
        // REPORT: sampling.cost.insert (stop)
        getContext().writeIntoReport("sampling.cost.insert", timer.stop());
        // REPORT: sampling.count.insert
        getContext().writeIntoReport("sampling.count.insert", insertedCount);

        // REPORT: sampling.cost.update-sample (start)
        timer = AdaTimer.create();
        long updatedCount = cleanedCount + insertedCount;
        double vprob = 1.0 * updatedCount / (sample.tableSize + (long) adaBatch.getSize());
        Dataset<Row> updatedSample = cleanedSample.union(insertedSample.withColumn("verdict_vprob", lit(vprob)));
        updatedCount = updatedSample.count();
        // REPORT: sampling.cost.update-sample (stop)
        getContext().writeIntoReport("sampling.cost.update-sample", timer.stop());

        // REPORT: sampling.cost.save-sample (start)
        timer = AdaTimer.create();
        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName));
        updatedSample.write().saveAsTable(sample.tableName + "_tmp");
        getContext().getDbmsSpark2()
                .execute(String.format("DROP TABLE %s.%s", sample.schemaName, sample.tableName))
                .execute(String.format("ALTER TABLE %s_tmp RENAME TO %s", sample.tableName, sample.tableName));
        // REPORT: sampling.cost.save-sample (stop)
        getContext().writeIntoReport("sampling.cost.save-sample", timer.stop());

        // REPORT: sampling.cost.update-meta (start)
        timer = AdaTimer.create();
        SparkSession spark = getContext().getDbmsSpark2().getSparkSession();
        updateMetaInfo(sample,
                spark.createDataFrame(ImmutableList.of(new VerdictMetaSize(sample.schemaName, sample.tableName, updatedCount, sample.tableSize + (long) adaBatch.getSize())), VerdictMetaSize.class).toDF(),
                spark.createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, Math.round(10000.0 * updatedCount / (sample.tableSize + (long) adaBatch.getSize())) / 10000.0, sample.onColumn)), VerdictMetaName.class).toDF());
        // REPORT: sampling.cost.update-meta (stop)
        getContext().writeIntoReport("sampling.cost.update-meta", timer.stop());
    }

    private void updateStratified(Sample sample, AdaBatch adaBatch) {
        Random random = new Random();
        long x = findX(sample, adaBatch);
        TableEntity sampleTable = new TableEntity(sample.schemaName, sample.tableName);
        TableEntity batchTable = new TableEntity(adaBatch.getDbName(), adaBatch.getTableName());
        TableEntity originGroupTable = new TableEntity(sample.schemaName, String.format("ada_%s_group_%s", sample.originalTable, sample.onColumn));
        String batchGroupTable = "ada_" + RandomStringUtils.randomAlphanumeric(6) + "_group_" + sample.onColumn;
        String sampleGroupTable = "ada_" + RandomStringUtils.randomAlphanumeric(6) + "_group_" + sample.onColumn;

        // REPORT: sampling.cost.build-batch-group (start)
        AdaTimer timer = AdaTimer.create();
        Dataset<Row> batchGroupDF = getContext().getSamplingController()
                .buildGroupSizeTable(adaBatch.getDbName(), adaBatch.getTableName(), batchGroupTable, sample.onColumn);
        // REPORT: sampling.cost.build-batch-group (stop)
        getContext().writeIntoReport("sampling.cost.build-batch-group", timer.stop());

        // REPORT: sampling.cost.build-sample-group (start)
        timer = AdaTimer.create();
        Dataset<Row> sampleGroupDF = getContext().getSamplingController()
                .buildGroupSizeTable(sampleTable.getSchema(), sampleTable.getTable(), sampleGroupTable, sample.onColumn);
        // REPORT: sampling.cost.build-sample-group (stop)
        getContext().writeIntoReport("sampling.cost.build-batch-group", timer.stop());

        // REPORT: sampling.cost.find-sample-size (start)
        timer = AdaTimer.create();
        List<Long> sampleGroupInfoList = sampleGroupDF
                .map((MapFunction<Row, Long>) row -> (row.get(row.fieldIndex("group_size")) != null ? row.getLong(row.fieldIndex("group_size")) : 0), Encoders.LONG())
                .collectAsList();
        List<Long> batchGroupInfoList = batchGroupDF
                .map((MapFunction<Row, Long>) row -> (row.get(row.fieldIndex("group_size")) != null ? row.getLong(row.fieldIndex("group_size")) : 0), Encoders.LONG())
                .collectAsList();
        long eachKeptInSample = findEachGroupCardinality(sampleGroupInfoList, x, 0, x, 0.001);
        long eachKeptInBatch = findEachGroupCardinality(batchGroupInfoList, getContext().getSampleStatus(sample).getMaxExpectedSize() - x, 0, getContext().getSampleStatus(sample).getMaxExpectedSize() - x, 0.001);
        eachKeptInSample = Math.max(eachKeptInSample, 10);
        eachKeptInBatch = Math.max(eachKeptInBatch, 10);
        // REPORT: sampling.cost.find-sample-size (stop)
        getContext().writeIntoReport("sampling.cost.find-sample-size", timer.stop());

        // REPORT: sampling.cost.clean (start)
        timer = AdaTimer.create();
        String sqlForClean = String.format("SELECT u.* FROM %s AS u INNER JOIN %s AS v ON u.%s=v.%s WHERE Rand(Unix_timestamp())<%f/CAST(v.group_size AS DOUBLE)",
                sampleTable.toSQL(), sampleGroupTable, sample.onColumn, sample.onColumn, eachKeptInSample * 1.0);
        Dataset<Row> cleanedSample = getContext().getDbms()
                .execute(sqlForClean)
                .getResultSet()
                .drop("verdict_group_size")
                .drop("verdict_rand")
                .drop("verdict_vprob");
        AdaLogger.info(this, sample.toString() + " cleaned cardinality: " + cleanedSample.count());
        // REPORT: sampling.cost.clean (stop)
        getContext().writeIntoReport("sampling.cost.clean", timer.stop());

        // REPORT: sampling.cost.insert (start)
        timer = AdaTimer.create();
        String sqlForInsert = String.format("SELECT u.* FROM %s AS u INNER JOIN %s AS v ON u.%s=v.%s WHERE Rand(Unix_timestamp())<%f/CAST(v.group_size AS DOUBLE)",
                batchTable.toSQL(), batchGroupTable, sample.onColumn, sample.onColumn, eachKeptInBatch * 1.0);
        Dataset<Row> insertedSample = getContext().getDbms()
                .execute(sqlForInsert)
                .getResultSet()
                .withColumn("verdict_vpart", lit(Math.floor(random.nextDouble() * 100)));
        AdaLogger.info(this, sample.toString() + " inserted cardinality: " + insertedSample.count());
        // REPORT: sampling.cost.insert (stop)
        getContext().writeIntoReport("sampling.cost.insert", timer.stop());

        // REPORT: sampling.cost.union (start)
        timer = AdaTimer.create();
        Dataset<Row> updatedSample = cleanedSample.union(insertedSample).cache();
        String updatedSampleView = AdaNamespace.tempUniqueName("updated_sample");
        updatedSample.createOrReplaceTempView(updatedSampleView);
        long updatedCount = updatedSample.count();
        AdaLogger.info(this, sample.toString() + " updated cardinality: " + updatedCount);
        // REPORT: sampling.cost.union (stop)
        getContext().writeIntoReport("sampling.cost.union", timer.stop());

        // REPORT: sampling.cost.attach-prob (start)
        timer = AdaTimer.create();
        String sqlForGroup = String.format("SELECT (CASE WHEN (u.%s IS NULL) THEN v.%s ELSE u.%s END) AS group_name, (CASE WHEN (u.group_size IS NULL) THEN 0 ELSE u.group_size END), (CASE WHEN (v.group_size IS NULL) THEN 0 ELSE v.group_size END) AS v_group_size FROM %s AS u FULL OUTER JOIN %s AS v ON u.%s=v.%s",
                sample.onColumn, sample.onColumn, sample.onColumn, originGroupTable.toSQL(), batchGroupTable, sample.onColumn, sample.onColumn);
        long finalEachKeptInSample = eachKeptInSample;
        long finalEachKeptInBatch = eachKeptInBatch;
        Dataset<Row> updatedGroup = getContext().getDbms().execute(sqlForGroup).getResultSet()
                .map((MapFunction<Row, StratifiedAdaptiveGroup>) row -> {
                    String group_name = row.getString(row.fieldIndex("group_name"));
                    long u_group_size = row.getLong(row.fieldIndex("u_group_size"));
                    long v_group_size = row.getLong(row.fieldIndex("v_group_size"));
                    long realKeptInSample = Math.min(u_group_size, finalEachKeptInSample);
                    long realKeptInBatch = Math.min(v_group_size, finalEachKeptInBatch);
                    long group_size = u_group_size + v_group_size;
                    double verdict_vprob = 1.0 * (realKeptInSample + realKeptInBatch) / group_size;
                    return new StratifiedAdaptiveGroup(group_name, u_group_size, v_group_size, group_size, verdict_vprob);
                }, Encoders.bean(StratifiedAdaptiveGroup.class)).toDF().cache();
        updatedGroup.count();
        String updatedGroupView = AdaNamespace.tempUniqueName("updated_group");
        updatedGroup.createOrReplaceTempView(updatedGroupView);

        String sqlForProb = String.format("CREATE TABLE %s_tmp AS (SELECT u.*,v.verdict_vprob FROM %s AS u INNER JOIN %s AS v ON u.%s=v.%s)",
                sample.tableName, updatedSampleView, updatedGroupView, sample.onColumn, "group_name");
        getContext().getDbms()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(sqlForProb);
        // REPORT: sampling.cost.attach-prob (stop)
        getContext().writeIntoReport("sampling.cost.attach-prob", timer.stop());

        // update origin group table
        String sqlForUpdateGroupTable = String.format("CREATE TABLE %s_tmp AS SELECT group_name AS %s, (u_group_size+v_group_size) AS group_size FROM %s",
                originGroupTable.getTable(), sample.onColumn, updatedGroupView);
        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(sqlForUpdateGroupTable)
                .execute(String.format("DROP TABLE IF EXISTS %s", originGroupTable.getTable()))
                .execute(String.format("ALTER TABLE %s_tmp RENAME TO %s", originGroupTable.getTable(), originGroupTable.getTable()));

        // update origin sample table
        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(String.format("DROP TABLE IF EXISTS %s.%s", sample.schemaName, sample.tableName))
                .execute(String.format("ALTER TABLE %s_tmp RENAME TO %s", sample.tableName, sample.tableName));

        updateMetaInfo(sample,
                getContext().getDbms().getSparkSession()
                .createDataFrame(ImmutableList.of(new VerdictMetaSize(sample.schemaName, sample.tableName, updatedCount, getContext().getTableMeta().getCardinality())), VerdictMetaSize.class)
                .toDF(),
                getContext().getDbms().getSparkSession()
                .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, Math.round(10000.0 * updatedCount / (sample.tableSize + (long) adaBatch.getSize())) / 10000.0, sample.onColumn)), VerdictMetaName.class)
                .toDF());
    }

    private long findX(Sample sample, AdaBatch adaBatch) {
        Random random = new Random();
        long xMin = Math.max(getContext().getSampleStatus(sample).getMaxExpectedSize() - getContext().getSampleStatus(sample).M(), 0);
        long xMax = sample.sampleSize;
        return Math.round(random.nextDouble() * (xMax - xMin)) + xMin;
    }

    private long findEachGroupCardinality(List<Long> groupInfoList, long expectedCardinality, long minCardinality, long maxCardinality, double precision) {
        long eachGroupCardinality = 0;
        while (minCardinality <= maxCardinality) {
            long realCardinality = 0;
            maxCardinality = (minCardinality + maxCardinality) >> 1;
            for (Long count : groupInfoList) {
                realCardinality += Math.min(count, eachGroupCardinality);
            }
            if (realCardinality < expectedCardinality * (1 - precision)) {
                minCardinality = eachGroupCardinality + 1;
            } else if (realCardinality > expectedCardinality * (1 + precision)) {
                maxCardinality = eachGroupCardinality - 1;
            } else {
                break;
            }
        }
        return eachGroupCardinality;
    }

    @Override
    public String name() {
        return "adaptive";
    }

    @Override
    public String nameInPaper() {
        return "ARS";
    }
}
