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
//        getContext().getDbmsSpark2().getSparkSession().sparkContext().parallelize()
        AdaLogger.info(this, "Chosen key set size: " + chosen.keySet().size());
        AdaLogger.info(this, "Chosen value set size: " + chosen.values().size());

//        HashSet<Long> outdated = new HashSet<>(chosen.keySet());
//        HashSet<Integer> inserted = new HashSet<>(chosen.values());
        sample.setRows(getContext().getDbmsSpark2()
                .execute(String.format("SELECT * FROM %s.%s", sample.schemaName, sample.tableName))
                .getResultSet());
        Dataset<Row> originSample = sample.getRows()
                .withColumn("id", monotonically_increasing_id());
//                .filter((FilterFunction<Row>) row -> !outdated.contains(row.getLong(row.fieldIndex("id"))));
        List<SampleRowStatus> outdated = Lists.newArrayList();
        for (long position: chosen.keySet()) {
            outdated.add(new SampleRowStatus(position));
        }
        Dataset<Row> expiredSample = spark.createDataFrame(outdated, SampleRowStatus.class);
        Dataset<Row> cleanedSample = originSample
                .join(expiredSample, originSample.col("id").$eq$eq$eq(expiredSample.col("id")), "left_outer");
        cleanedSample = cleanedSample
                .filter(cleanedSample.col("status").isNull())
                .drop("status")
                .drop("id");

        AdaLogger.info(this, "Sample cleaned row count: " + cleanedSample.count());

        List<SampleRowStatus> inserted = Lists.newArrayList();
        for (int position: chosen.values()) {
            inserted.add(new SampleRowStatus(position));
        }
        Dataset<Row> unexpiredSample = spark.createDataFrame(inserted, SampleRowStatus.class);
        Dataset<Row> insertedSample = getContext().getDbmsSpark2()
                .execute(String.format("SELECT * FROM %s.%s", adaBatch.getDbName(), adaBatch.getTableName()))
                .getResultSet()
                .withColumn("id", monotonically_increasing_id())
                .withColumn("verdict_rand", when(col("page_count").$greater$eq(0), randomGenerator.nextDouble() * sampleSize / tableSize))
                .withColumn("verdict_vpart", when(col("page_count").$greater$eq(0),  Math.floor(randomGenerator.nextDouble() * 100)))
                .withColumn("verdict_vprob", lit(sample.samplingRatio));
//                .filter((FilterFunction<Row>) row -> inserted.contains((int) row.getLong(row.fieldIndex("id"))));
        insertedSample = insertedSample
                .join(unexpiredSample, insertedSample.col("id").$eq$eq$eq(unexpiredSample.col("id")), "left_outer");
        insertedSample = insertedSample.filter(insertedSample.col("status").isNull())
                .drop("status")
                .drop("id");
        Dataset<Row> updatedSample = cleanedSample
                .union(insertedSample)
                .drop("verdict_vprob")
                .withColumn("verdict_vprob", lit(1.0 * sample.sampleSize / (sample.tableSize + (long) adaBatch.getSize())));

//            getContext().getDbmsSpark2().execute(String.format("CREATE TABLE %s.%s%s LIKE %s.%s",
//                    sample.schemaName, sample.tableName, "_ada", sample.schemaName, sample.tableName));

        getContext().getDbmsSpark2().execute(String.format("USE %s", sample.schemaName));
        updatedSample.write().saveAsTable(sample.tableName + "_tmp");

        getContext().getDbmsSpark2()
                .execute(String.format("USE %s", sample.schemaName))
//                .execute(String.format("TRUNCATE TABLE %s.%s", sample.schemaName, sample.tableName))
                .execute(String.format("DROP TABLE %s.%s", sample.schemaName, sample.tableName))
                .execute(String.format("ALTER TABLE %s_tmp RENAME TO %s", sample.tableName, sample.tableName));
//        sampleUpdated.write().insertInto(sample.tableName);
//        AdaLogger.info(this, "Sample updated size is: " + sampleUpdated.count());

//        sampleUpdated.write().saveAsTable(sample.tableName);

        List<Sample> samples = getSamples();
        List<Dataset<Row>> metaSizeDFs = Lists.newArrayList();
        List<Dataset<Row>> metaNameDFs = Lists.newArrayList();
        for (Sample _sample : samples) {
            Dataset<Row> metaSizeDF;
            Dataset<Row> metaNameDF;
            if (Math.abs(_sample.samplingRatio - sample.samplingRatio) < 0.00001) {
                metaSizeDF = spark
                        .createDataFrame(ImmutableList.of(new VerdictMetaSize(sample.schemaName, sample.tableName, sample.sampleSize, sample.tableSize + (long) adaBatch.getSize())), VerdictMetaSize.class)
                        .toDF();
                metaNameDF = spark
                        .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, Math.round(100.0 * sample.sampleSize / (sample.tableSize + (long) adaBatch.getSize())) / 100.0, sample.onColumn)), VerdictMetaName.class)
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
        metaNameDF.write().saveAsTable("verdict_meta_name");
        metaSizeDF.write().saveAsTable("verdict_meta_size");
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
