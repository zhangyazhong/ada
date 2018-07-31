package daslab.sampling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import daslab.bean.*;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;
import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * @author zyz
 * @version 2018-05-16
 */
@SuppressWarnings("Duplicates")
public class ReservoirSampling extends SamplingStrategy {
    public ReservoirSampling(AdaContext context) {
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
    }

    private void updateUniform(Sample sample, AdaBatch adaBatch) {
        Random randomGenerator = new Random();
        SparkSession spark = getContext().getDbmsSpark2().getSparkSession();
        Map<Long, Integer> chosen = Maps.newHashMap();
        String sampleSchema = sample.schemaName;
        long tableSize = sample.tableSize;
        long sampleSize = sample.sampleSize;
        for (int i = 0; i < adaBatch.getSize(); i++) {
            long totalSize = tableSize + (long) i;
            long position = (long) Math.floor(randomGenerator.nextDouble() * totalSize);
            if (position < sampleSize) {
                chosen.put(position, i);
            }
        }
        AdaLogger.info(this, "Chosen key set size: " + chosen.keySet().size());
        AdaLogger.info(this, "Chosen value set size: " + chosen.values().size());

        sample.setRows(getContext().getDbmsSpark2()
                .execute(String.format("SELECT * FROM %s.%s", sample.schemaName, sample.tableName))
                .getResultSet());
        Dataset<Row> originSample = sample.getRows();
        Dataset<Row> cleanedSample = originSample.sample(false, 1.0 * (sampleSize - chosen.keySet().size()) / sampleSize);
        long cleanedCount = cleanedSample.count();

        AdaLogger.info(this, "Sample cleaned row count: " + cleanedCount);

        Dataset<Row> insertedSample = getContext().getDbmsSpark2()
                .execute(String.format("SELECT * FROM %s.%s", adaBatch.getDbName(), adaBatch.getTableName()))
                .getResultSet()
                .withColumn("verdict_rand", when(col("page_count").$greater$eq(0), randomGenerator.nextDouble() * sampleSize / tableSize))
                .withColumn("verdict_vpart", when(col("page_count").$greater$eq(0),  Math.floor(randomGenerator.nextDouble() * 100)))
                .withColumn("verdict_vprob", lit(sample.samplingRatio));
        insertedSample = insertedSample.sample(false, 1.0 * chosen.keySet().size() / adaBatch.getSize());
        long insertedCount = insertedSample.count();

        AdaLogger.info(this, "Sample inserted row count: " + insertedCount);

        long updatedCount = cleanedCount + insertedCount;
        Dataset<Row> updatedSample = cleanedSample
                .union(insertedSample)
                .drop("verdict_vprob")
                .withColumn("verdict_vprob", lit(1.0 * updatedCount / (sample.tableSize + (long) adaBatch.getSize())));

        getContext().getDbmsSpark2().execute(String.format("USE %s", sample.schemaName));
        updatedSample.write().saveAsTable(sample.tableName + "_tmp");

        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(String.format("DROP TABLE %s.%s", sample.schemaName, sample.tableName))
                .execute(String.format("ALTER TABLE %s_tmp RENAME TO %s", sample.tableName, sample.tableName));

        List<Sample> samples = getSamples(true);
        List<Dataset<Row>> metaSizeDFs = Lists.newArrayList();
        List<Dataset<Row>> metaNameDFs = Lists.newArrayList();
        for (Sample _sample : samples) {
            Dataset<Row> metaSizeDF;
            Dataset<Row> metaNameDF;
            if (Math.abs(_sample.samplingRatio - sample.samplingRatio) < 0.00001 && _sample.sampleType.equals(sample.sampleType) && _sample.onColumn.equals(sample.onColumn)) {
                VerdictMetaSize metaSize = new VerdictMetaSize(sample.schemaName, sample.tableName, updatedCount, sample.tableSize + (long) adaBatch.getSize());
                VerdictMetaName metaName = new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, Math.round(100.0 * updatedCount / (sample.tableSize + (long) adaBatch.getSize())) / 100.0, sample.onColumn);
                AdaLogger.debug(this, "Updated meta size: " + metaSize.toString());
                AdaLogger.debug(this, "Updated meta name: " + metaName.toString());
                metaSizeDF = spark
                        .createDataFrame(ImmutableList.of(metaSize), VerdictMetaSize.class)
                        .toDF();
                metaNameDF = spark
                        .createDataFrame(ImmutableList.of(metaName), VerdictMetaName.class)
                        .toDF();
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
        Dataset<Row> metaSizeDF = metaSizeDFs.get(0);
        Dataset<Row> metaNameDF = metaNameDFs.get(0);
        for (int i = 1; i < metaNameDFs.size(); i++) {
            metaSizeDF = metaSizeDF.union(metaSizeDFs.get(i));
            metaNameDF = metaNameDF.union(metaNameDFs.get(i));
        }
        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sampleSchema))
                .execute(String.format("DROP TABLE %s.%s", sampleSchema, "verdict_meta_name"))
                .execute(String.format("DROP TABLE %s.%s", sampleSchema, "verdict_meta_size"));
        metaNameDF.select("originalschemaname", "originaltablename", "sampleschemaaname", "sampletablename", "sampletype", "samplingratio", "columnnames").write().saveAsTable("verdict_meta_name");
        metaSizeDF.select("schemaname", "tablename", "samplesize", "originaltablesize").write().saveAsTable("verdict_meta_size");
    }

    private void updateStratified(Sample sample, AdaBatch adaBatch) {
        Random randomGenerator = new Random();
        SparkSession spark = getContext().getDbmsSpark2().getSparkSession();
        String uniqueString = RandomStringUtils.randomAlphanumeric(6);
        // build batch group table
        getContext().getSamplingController().buildGroupSizeTable(adaBatch.getDbName(), adaBatch.getTableName(), sample.schemaName,"ada_" + uniqueString + "_group_" + sample.onColumn, sample.onColumn);

        String onColumn = sample.onColumn;
        String originGroupTable = String.format("%s.ada_%s_group_%s", sample.schemaName, sample.originalTable, onColumn);
        String batchGroupTable = String.format("%s.ada_%s_group_%s", sample.schemaName, uniqueString, onColumn);
        String originSampleTable = String.format("%s.%s", sample.schemaName, sample.tableName);
        String originBatchTable = String.format("%s.%s", adaBatch.getDbName(), adaBatch.getTableName());
        String groupInfoSQL = String.format(
                "SELECT a.%s AS a_group_name, (CASE WHEN (a.group_size IS NULL) THEN 0 ELSE a.group_size END) AS a_group_size, b.%s AS b_group_name, (CASE WHEN (b.group_size IS NULL) THEN 0 ELSE b.group_size END) AS b_group_size, c.%s AS c_group_name, (CASE WHEN (c.group_size IS NULL) THEN 0 ELSE c.group_size END) AS c_group_size " +
                        "FROM %s a " +
                        "FULL OUTER JOIN %s b ON a.%s=b.%s " +
                        "FULL OUTER JOIN (SELECT %s, COUNT(*) AS group_size FROM %s GROUP BY %s) c ON a.%s=c.%s",
                sample.onColumn, sample.onColumn, sample.onColumn,
                originGroupTable,
                batchGroupTable, sample.onColumn, sample.onColumn,
                sample.onColumn, originSampleTable, sample.onColumn, sample.onColumn, sample.onColumn);
        Dataset<Row> groupInfoDF = getContext().getDbms().execute(groupInfoSQL).getResultSet();
        long groupCount = groupInfoDF.count();
        groupInfoDF.createOrReplaceTempView("group_joined_info");

        long sampleCardinality = sample.sampleSize;
        List<Long> groupInfoList = groupInfoDF.map((MapFunction<Row, Long>) row ->
                (row.get(row.fieldIndex("a_group_size")) != null ? row.getLong(row.fieldIndex("a_group_size")) : 0)
                + (row.get(row.fieldIndex("b_group_size")) != null ? row.getLong(row.fieldIndex("b_group_size")) : 0),
                Encoders.bean(Long.class)).collectAsList();
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

        final long finalEachGroupSampleCardinality = Math.max(eachGroupSampleCardinality, 10);
        Dataset<Row> groupExchangeInfoDF = groupInfoDF.map((MapFunction<Row, StratifiedJoinedGroup>) row -> {
            String groupName = row.getString(row.fieldIndex("a_group_name"));
            long aGroupSize = row.get(row.fieldIndex("a_group_size")) != null ? row.getLong(row.fieldIndex("a_group_size")) : 0;
            long bGroupSize = row.get(row.fieldIndex("b_group_size")) != null ? row.getLong(row.fieldIndex("b_group_size")) : 0;
            long cGroupSize = row.get(row.fieldIndex("c_group_size")) != null ? row.getLong(row.fieldIndex("c_group_size")) : 0;
            long aExchangeSize = Math.max(0, cGroupSize - finalEachGroupSampleCardinality);
            long bExchangeSize = Math.max(0, finalEachGroupSampleCardinality - cGroupSize);
            Set<Long> exchangeSet = Sets.newHashSet();
            long tableSize = Math.max(aGroupSize, finalEachGroupSampleCardinality);
            for (long i = bExchangeSize; i < adaBatch.getSize(); i++) {
                long totalSize = tableSize + i;
                long position = (long) Math.floor(randomGenerator.nextDouble() * totalSize);
                if (position < finalEachGroupSampleCardinality) {
                    exchangeSet.add(position);
                }
            }
            aExchangeSize = aExchangeSize + exchangeSet.size() * Math.min(1, cGroupSize / finalEachGroupSampleCardinality);
            bExchangeSize = bExchangeSize + exchangeSet.size() * Math.min(1, cGroupSize / finalEachGroupSampleCardinality);
            double aExchangeRatio = Math.max(0, 1.0 * (cGroupSize - aExchangeSize) / cGroupSize);
            double bExchangeRatio = Math.min(1, 1.0 * bExchangeSize / bGroupSize);
            double verdictProb = 1.0 * (cGroupSize - aExchangeSize + bExchangeSize) / (aGroupSize + bGroupSize);
            return new StratifiedJoinedGroup(groupName, aGroupSize, bGroupSize, aExchangeSize, bExchangeSize, aExchangeRatio, bExchangeRatio, verdictProb);
        }, Encoders.bean(StratifiedJoinedGroup.class)).toDF();
        groupExchangeInfoDF.createOrReplaceTempView("group_exchange_info");

        String sqlForClean = String.format("SELECT u.*, Rand(Unix_timestamp()) AS ada_rand FROM %s AS u INNER JOIN %s AS v ON u.%s=v.%s WHERE ada_rand<v.a_exchange_ratio",
                originSampleTable, "group_exchange_info", sample.onColumn, "group_name");
        Dataset<Row> cleanedSample = getContext().getDbms()
                .execute(sqlForClean)
                .getResultSet()
                .drop("ada_rand", "verdict_vprob");
        String sqlForInsert = String.format("SELECT u.*, Rand(Unix_timestamp()) AS ada_rand FROM %s AS u INNER JOIN %s AS v ON u.%s=v.%s WHERE ada_rand<v.b_exchange_ratio",
                originBatchTable, "group_exchange_info", sample.onColumn, "group_name");
        Dataset<Row> insertedSample = getContext().getDbms()
                .execute(sqlForInsert)
                .getResultSet()
                .drop("ada_rand")
                .withColumn("verdict_vpart", when(col("page_count").$greater$eq(0), Math.floor(randomGenerator.nextDouble() * 100)));
        Dataset<Row> updatedSample = cleanedSample
                .union(insertedSample);
        updatedSample.createOrReplaceTempView("updated_sample");
        String sqlForProb = String.format("SELECT m.*, n.verdict_vprob AS verdict_vprob FROM %s m INNER JOIN %s n ON m.%s=n.group_name",
                "updated_sample", "group_exchange_info", sample.onColumn);
        Dataset<Row> updatedSampleWithProb = getContext().getDbms()
                .execute(sqlForProb)
                .getResultSet();
        getContext().getDbmsSpark2().execute(String.format("USE %s", sample.schemaName));
        updatedSampleWithProb.write().saveAsTable(sample.tableName + "_tmp");

        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(String.format("DROP TABLE %s.%s", sample.schemaName, sample.tableName))
                .execute(String.format("ALTER TABLE %s_tmp RENAME TO %s", sample.tableName, sample.tableName));

        long updatedCount = updatedSampleWithProb.count();
        List<Sample> samples = getSamples(true);
        List<Dataset<Row>> metaSizeDFs = Lists.newArrayList();
        List<Dataset<Row>> metaNameDFs = Lists.newArrayList();
        for (Sample _sample : samples) {
            Dataset<Row> metaSizeDF;
            Dataset<Row> metaNameDF;
            if (Math.abs(_sample.samplingRatio - sample.samplingRatio) < 0.00001 && _sample.sampleType.equals(sample.sampleType) && _sample.onColumn.equals(sample.onColumn)) {
                double ratio = Math.round(sample.samplingRatio * 100) / 100;
                VerdictMetaSize metaSize = new VerdictMetaSize(sample.schemaName, sample.tableName, updatedCount, getContext().getTableMeta().getCardinality());
                VerdictMetaName metaName = new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, ratio, sample.onColumn);
                AdaLogger.debug(this, "Updated meta size: " + metaSize.toString());
                AdaLogger.debug(this, "Updated meta name: " + metaName.toString());
                metaSizeDF = spark
                        .createDataFrame(ImmutableList.of(metaSize), VerdictMetaSize.class)
                        .toDF();
                metaNameDF = spark
                        .createDataFrame(ImmutableList.of(metaName), VerdictMetaName.class)
                        .toDF();
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
        Dataset<Row> metaSizeDF = metaSizeDFs.get(0);
        Dataset<Row> metaNameDF = metaNameDFs.get(0);
        for (int i = 1; i < metaNameDFs.size(); i++) {
            metaSizeDF = metaSizeDF.union(metaSizeDFs.get(i));
            metaNameDF = metaNameDF.union(metaNameDFs.get(i));
        }
        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(String.format("DROP TABLE %s.%s", sample.schemaName, "verdict_meta_name"))
                .execute(String.format("DROP TABLE %s.%s", sample.schemaName, "verdict_meta_size"));
        metaNameDF.select("originalschemaname", "originaltablename", "sampleschemaaname", "sampletablename", "sampletype", "samplingratio", "columnnames").write().saveAsTable("verdict_meta_name");
        metaSizeDF.select("schemaname", "tablename", "samplesize", "originaltablesize").write().saveAsTable("verdict_meta_size");

        // update origin group table
        String sqlForUpdateGroupTable = String.format("CREATE TABLE %s_tmp AS SELECT a_group_name AS %s, (a_group_size+b_group_size) AS group_size FROM %s", originGroupTable, sample.onColumn, "group_joined_info");
        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
                .execute(sqlForUpdateGroupTable)
                .execute(String.format("DROP TABLE %s", originGroupTable))
                .execute(String.format("ALTER TABLE %s_tmp RENAME TO %s", originGroupTable, originGroupTable));
    }

    @Override
    public String name() {
        return "reservoir";
    }
}
