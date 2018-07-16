package daslab.sampling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import daslab.bean.AdaBatch;
import daslab.bean.Sample;
import daslab.bean.VerdictMetaName;
import daslab.bean.VerdictMetaSize;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.spark.sql.Dataset;
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
    public VerdictSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(Sample sample, AdaBatch adaBatch) {
        try {
            VerdictSpark2Context verdictSpark2Context = new VerdictSpark2Context(getContext().getDbmsSpark2().getSparkSession().sparkContext());
            AdaLogger.info(this, "About to drop all samples.");
            /*
            verdictSpark2Context.sql(String.format("DROP SAMPLES OF %s.%s",
                    getContext().get("dbms.default.database"), getContext().get("dbms.data.table")));
            for (Sample sample : samples) {
                AdaLogger.info(this, "About to create sample with sampling ratio " + sample.samplingRatio + " of " +  getContext().get("dbms.default.database") + "." + getContext().get("dbms.data.table"));
                verdictSpark2Context.sql("CREATE " + (sample.samplingRatio * 100) + "% UNIFORM SAMPLE OF " + getContext().get("dbms.default.database") + "." + getContext().get("dbms.data.table"));
            }
            */
            verdictSpark2Context.sql(String.format("DROP %d%% SAMPLES OF %s.%s",
                    Math.round(sample.samplingRatio * 100),
                    getContext().get("dbms.default.database"), getContext().get("dbms.data.table")));
            AdaLogger.info(this, "About to create sample with sampling ratio " + sample.samplingRatio + " of " +  getContext().get("dbms.default.database") + "." + getContext().get("dbms.data.table"));
            verdictSpark2Context.sql("CREATE " + Math.round(sample.samplingRatio * 100) + "% UNIFORM SAMPLE OF " + getContext().get("dbms.default.database") + "." + getContext().get("dbms.data.table"));
        } catch (VerdictException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void update(Sample sample, AdaBatch adaBatch) {
    }

    @Override
    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
        try {
            VerdictSpark2Context verdictSpark2Context = getContext().getVerdict();
            AdaLogger.info(this, "About to drop sample with ratio " + sample.samplingRatio);
            /*
            AdaLogger.info(this, "About to run: " + String.format("DROP %d%% SAMPLES OF %s.%s",
                    round(sample.samplingRatio * 100),
                    getContext().get("dbms.default.database"), getContext().get("dbms.data.table")));
            verdictSpark2Context.sql(String.format("DROP %d%% SAMPLES OF %s.%s",
                    round(sample.samplingRatio * 100),
                    getContext().get("dbms.default.database"), getContext().get("dbms.data.table")));
            */
            deleteSampleTable(sample);
            deleteSampleMeta(sample);
            AdaLogger.info(this, String.format("About to create sample with sampling ratio %f of %s.%s", round(ratio * 100) / 100.0, getContext().get("dbms.default.database"), getContext().get("dbms.data.table")));
            verdictSpark2Context.sql("CREATE " + (int) round(ratio * 100) + "% UNIFORM SAMPLE OF " + getContext().get("dbms.default.database") + "." + getContext().get("dbms.data.table"));
        } catch (VerdictException e) {
            e.printStackTrace();
        }
    }

    private void deleteSampleTable(Sample sample) {
        String sampleSchema = sample.schemaName;
        String sampleTable = sample.tableName;
        getContext().getDbmsSpark2().execute(String.format("DROP TABLE IF EXISTS %s.%s", sampleSchema, sampleTable));
    }

    private void deleteSampleMeta(Sample sample) {
        SparkSession spark = getContext().getDbmsSpark2().getSparkSession();
        List<Sample> samples = getSamples();
        List<Dataset<Row>> metaSizeDFs = Lists.newArrayList();
        List<Dataset<Row>> metaNameDFs = Lists.newArrayList();
        for (Sample _sample : samples) {
            Dataset<Row> metaSizeDF;
            Dataset<Row> metaNameDF;
            if (Math.abs(_sample.samplingRatio - sample.samplingRatio) > 0.00001) {
                metaSizeDF = spark
                        .createDataFrame(ImmutableList.of(new VerdictMetaSize(sample.schemaName, sample.tableName, sample.sampleSize, sample.tableSize)), VerdictMetaSize.class)
                        .toDF();
                metaNameDF = spark
                        .createDataFrame(ImmutableList.of(new VerdictMetaName(getContext().get("dbms.default.database"), sample.originalTable, sample.schemaName, sample.tableName, sample.sampleType, sample.samplingRatio, sample.onColumn)), VerdictMetaName.class)
                        .toDF();
                metaSizeDFs.add(metaSizeDF);
                metaNameDFs.add(metaNameDF);
            }
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

    }

    @Override
    public String name() {
        return "verdict sampling";
    }
}
