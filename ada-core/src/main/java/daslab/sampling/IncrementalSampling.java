package daslab.sampling;

import com.google.common.collect.ImmutableList;
import daslab.bean.*;
import daslab.context.AdaContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Random;

import static org.apache.spark.sql.functions.lit;

/**
 * @author zyz
 * @version 2018-06-05
 */
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
        run(sample, adaBatch);
    }

    @Override
    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
        run(sample, adaBatch);
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
