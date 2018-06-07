package daslab.sampling;

import daslab.bean.Batch;
import daslab.bean.PageCountSample;
import daslab.bean.Sample;
import daslab.context.AdaContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.List;
import java.util.Random;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * @author zyz
 * @version 2018-06-05
 */
public class IncrementalSampling extends SamplingStrategy {
    public IncrementalSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(Batch batch) {
        Random randomGenerator = new Random();
        List<Sample> samples = getSamples();
        for (Sample sample : samples) {
            Dataset<Row> batchDF = getContext().getDbmsSpark2()
                    .execute(String.format("SELECT * FROM %s.%s", batch.getDbName(), batch.getTableName()))
                    .getResultSet()
                    .withColumn("verdict_rand", when(col("page_count").$greater$eq(0), randomGenerator.nextDouble()))
                    .withColumn("verdict_vpart", when(col("page_count").$greater$eq(0),  Math.floor(randomGenerator.nextDouble() * 100)))
                    .filter(col("verdict_rand").$less$eq(sample.samplingRatio));
            Dataset<Row> sampleDF = getContext().getDbmsSpark2()
                    .execute(String.format("SELECT * FROM %s.%s", sample.schemaName, sample.tableName))
                    .getResultSet();
            long cardinality = getContext().getDbmsSpark2()
                    .execute(String.format("SELECT COUNT(*) AS count FROM %s.%s", getContext().get("dbms.default.database"), getContext().get("dbms.data.table")))
                    .getResultAsLong(0, "count");
            long totalSize = sampleDF.count() + batchDF.count();
            double totalRatio = 1.0 * totalSize / cardinality;
            sampleDF = batchDF
                    .withColumn("verdict_vprob", lit(totalRatio))
                    .union(getContext().getDbmsSpark2().getSparkSession().createDataFrame(sampleDF.javaRDD().map(row -> RowFactory.create(
                            row.get(row.fieldIndex("date_time")),
                            row.get(row.fieldIndex("project_name")),
                            row.get(row.fieldIndex("page_name")),
                            row.get(row.fieldIndex("page_count")),
                            row.get(row.fieldIndex("page_size")),
                            row.get(row.fieldIndex("verdict_rand")),
                            row.get(row.fieldIndex("verdict_vpart")),
                            totalRatio)), PageCountSample.class));
            getContext().getDbmsSpark2()
                    .execute(String.format("USE %s", sample.schemaName))
                    .execute(String.format("TRUNCATE TABLE %s.%s", sample.schemaName, sample.tableName));
            sampleDF.write().insertInto(sample.tableName);
        }
    }

    @Override
    public String name() {
        return "incremental";
    }
}
