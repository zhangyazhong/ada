package daslab.sampling;

import com.google.common.collect.Maps;
import daslab.bean.Batch;
import daslab.bean.Sample;
import daslab.context.AdaContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
    public void run(Batch batch) {
        Random randomGenerator = new Random();
        List<Sample> samples = getSamples();
        Map<Long, Integer> chosen = Maps.newHashMap();
        for (Sample sample : samples) {
            long tableSize = sample.tableSize;
            long sampleSize = sample.sampleSize;
            for (int i = 0; i < batch.getSize(); i++) {
                long totalSize = tableSize + (long) i;
                long position = (long) Math.floor(randomGenerator.nextDouble() * totalSize);
                if (position < sampleSize) {
                    chosen.put(position, i);
                }
            }
            Set<Long> outdated = chosen.keySet();
            Set<Integer> inserted = new HashSet<>(chosen.values());
            sample.setRows(getContext().getDbmsSpark2()
                    .execute(String.format("SELECT * FROM %s.%s", sample.schemaName, sample.tableName))
                    .getResultSet());
            Dataset<Row> sampleCleaned = sample.getRows()
                    .withColumn("id", monotonically_increasing_id())
                    .filter((FilterFunction<Row>) row -> !outdated.contains(row.getLong(row.fieldIndex("id"))));
            Dataset<Row> sampleInserted = getContext().getDbmsSpark2()
                    .execute(String.format("SELECT * FROM %s.%s", batch.getDbName(), batch.getTableName()))
                    .getResultSet()
                    .withColumn("id", monotonically_increasing_id())
                    .withColumn("verdict_rand", when(col("page_count").$greater$eq(0), randomGenerator.nextDouble() * sampleSize / tableSize))
                    .withColumn("verdict_vpart", when(col("page_count").$greater$eq(0),  Math.floor(randomGenerator.nextDouble() * 100)))
                    .withColumn("verdict_vprob", lit(sample.samplingRatio))
                    .filter((FilterFunction<Row>) row -> inserted.contains((int) row.getLong(row.fieldIndex("id"))));
            Dataset<Row> sampleUpdated = sampleCleaned.union(sampleInserted).drop("id");
//            getContext().getDbmsSpark2().execute(String.format("CREATE TABLE %s.%s%s LIKE %s.%s",
//                    sample.schemaName, sample.tableName, "_ada", sample.schemaName, sample.tableName));
            getContext().getDbmsSpark2()
                    .execute(String.format("USE %s", sample.schemaName))
                    .execute(String.format("TRUNCATE TABLE %s.%s", sample.schemaName, sample.tableName));
            sampleUpdated.write().insertInto(sample.tableName);
        }
    }

    @Override
    public String strategyName() {
        return "reservoir";
    }
}
