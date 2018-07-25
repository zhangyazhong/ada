package daslab.sampling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.bean.*;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * @author zyz
 * @version 2018-05-16
 */
public class ReservoirSampling extends SamplingStrategy {
    public ReservoirSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(Sample sample, AdaBatch adaBatch) {
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
            if (Math.abs(_sample.samplingRatio - sample.samplingRatio) < 0.00001) {
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
                        .createDataFrame(ImmutableList.of(new VerdictMetaSize(sample.schemaName, sample.tableName, sample.sampleSize, sample.tableSize)), VerdictMetaSize.class)
                        .toDF();
                metaNameDF = spark
                        .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, sample.samplingRatio, sample.onColumn)), VerdictMetaName.class)
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

    @Override
    public void update(Sample sample, AdaBatch adaBatch) {
        run(sample, adaBatch);
    }

    @Override
    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
    }

    @Override
    public String name() {
        return "reservoir";
    }
}
