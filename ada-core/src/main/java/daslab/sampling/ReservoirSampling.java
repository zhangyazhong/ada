package daslab.sampling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.bean.AdaBatch;
import daslab.bean.Sample;
import daslab.bean.VerdictMetaName;
import daslab.bean.VerdictMetaSize;
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
    public void run(AdaBatch adaBatch) {
        Random randomGenerator = new Random();
        List<Sample> samples = getSamples();
        Map<Long, Integer> chosen = Maps.newHashMap();
        String sampleSchema = getContext().get("dbms.default.database") + "_verdict";
        for (Sample sample : samples) {
            long tableSize = sample.tableSize;
            long sampleSize = sample.sampleSize;
            sampleSchema = sample.schemaName;
            for (int i = 0; i < adaBatch.getSize(); i++) {
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
                    .execute(String.format("SELECT * FROM %s.%s", adaBatch.getDbName(), adaBatch.getTableName()))
                    .getResultSet()
                    .withColumn("id", monotonically_increasing_id())
                    .withColumn("verdict_rand", when(col("page_count").$greater$eq(0), randomGenerator.nextDouble() * sampleSize / tableSize))
                    .withColumn("verdict_vpart", when(col("page_count").$greater$eq(0),  Math.floor(randomGenerator.nextDouble() * 100)))
                    .withColumn("verdict_vprob", lit(sample.samplingRatio))
                    .filter((FilterFunction<Row>) row -> inserted.contains((int) row.getLong(row.fieldIndex("id"))));
            Dataset<Row> sampleUpdated = sampleCleaned
                    .union(sampleInserted)
                    .drop("id")
                    .drop("verdict_vprob")
                    .withColumn("verdict_vprob", lit(1.0 * sample.sampleSize / (sample.tableSize + (long) adaBatch.getSize())));
//            getContext().getDbmsSpark2().execute(String.format("CREATE TABLE %s.%s%s LIKE %s.%s",
//                    sample.schemaName, sample.tableName, "_ada", sample.schemaName, sample.tableName));
            getContext().getDbmsSpark2()
                    .execute(String.format("USE %s", sample.schemaName))
                    .execute(String.format("TRUNCATE TABLE %s.%s", sample.schemaName, sample.tableName));
            sampleUpdated.write().insertInto(sample.tableName);
        }
        List<Dataset<Row>> metaSizeDFs = Lists.newArrayList();
        List<Dataset<Row>> metaNameDFs = Lists.newArrayList();
        for (Sample sample : samples) {
            Dataset<Row> metaSizeDF = getContext().getDbmsSpark2().getSparkSession()
                    .createDataFrame(ImmutableList.of(new VerdictMetaSize(sample.schemaName, sample.tableName, sample.sampleSize, sample.tableSize + (long) adaBatch.getSize())), VerdictMetaSize.class)
                    .toDF();
            Dataset<Row> metaNameDF = getContext().getDbmsSpark2().getSparkSession()
                    .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, 1.0 * sample.sampleSize / (sample.tableSize + (long) adaBatch.getSize()), sample.onColumn)), VerdictMetaName.class)
                    .toDF();
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
                .execute(String.format("TRUNCATE TABLE %s.%s", sampleSchema, "verdict_meta_name"))
                .execute(String.format("TRUNCATE TABLE %s.%s", sampleSchema, "verdict_meta_size"));
        metaNameDF.write().insertInto("verdict_meta_name");
        metaSizeDF.write().insertInto("verdict_meta_size");
    }

    @Override
    public String name() {
        return "reservoir";
    }
}
