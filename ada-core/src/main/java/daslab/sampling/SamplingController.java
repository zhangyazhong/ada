package daslab.sampling;

import daslab.bean.AdaBatch;
import daslab.bean.Sample;
import daslab.bean.SampleStatus;
import daslab.context.AdaContext;
import daslab.inspector.TableColumn;
import daslab.inspector.TableMeta;
import daslab.utils.AdaLogger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * @author zyz
 * @version 2018-05-15
 */
public class SamplingController {
    private AdaContext context;
    private SamplingStrategy samplingStrategy;
    private SamplingStrategy resamplingStrategy;

    public SamplingController(AdaContext context) {
        this.context = context;
        switch (context.get("sampling.strategy")) {
            case "verdict":
                this.samplingStrategy = new VerdictSampling(context);
                break;
            case "incremental":
                this.samplingStrategy = new IncrementalSampling(context);
                break;
            case "reservoir":
            default:
                this.samplingStrategy = new ReservoirSampling(context);
                break;
        }
        switch (context.get("resampling.strategy")) {
            case "incremental":
                this.samplingStrategy = new IncrementalSampling(context);
                break;
            case "verdict":
            default:
                this.resamplingStrategy = new VerdictSampling(context);
                break;
        }
        buildGroupSizeTable();
    }

    public SamplingStrategy getSamplingStrategy() {
        return samplingStrategy;
    }

    public SamplingStrategy getResamplingStrategy() {
        return resamplingStrategy;
    }

    public void update(Sample sample, AdaBatch adaBatch) {
        samplingStrategy.update(sample, adaBatch);
        samplingStrategy.getMetaSizes().forEach(verdictMetaSize ->
                AdaLogger.debug(this, "Final Meta Size: " + verdictMetaSize.toString()));
        samplingStrategy.getMetaNames().forEach(verdictMetaName ->
                AdaLogger.debug(this, "Final Meta Name: " + verdictMetaName.toString()));
    }

    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
        resamplingStrategy.resample(sample, adaBatch, ratio);
        resamplingStrategy.getMetaSizes().forEach(verdictMetaSize ->
                AdaLogger.debug(this, "Final Meta Size: " + verdictMetaSize.toString()));
        resamplingStrategy.getMetaNames().forEach(verdictMetaName ->
                AdaLogger.debug(this, "Final Meta Name: " + verdictMetaName.toString()));
    }

    public Map<Sample, SampleStatus> verify(Map<TableColumn, TableMeta.MetaInfo> metaMap, long tableSize) {
        return samplingStrategy.verify(metaMap, tableSize);
    }

    public void buildGroupSizeTable() {
        List<Sample> samples = samplingStrategy.getSamples(true);
        samples.stream().filter(sample -> sample.sampleType.equals("stratified")).forEach(sample -> {
                String onColumn = sample.onColumn;
                context.getDbms()
                        .execute(String.format("USE %s", context.get("dbms.verdict.database")))
                        .execute(String.format("CREATE TABLE ada_%s_group_%s AS SELECT %s, COUNT(*) AS group_size FROM %s.%s GROUP BY %s", sample.originalTable, onColumn, onColumn, context.get("dbms.default.database"), sample.originalTable, onColumn));
        });
    }

    public void buildGroupSizeTable(String originSchema, String originTable, String groupSchema, String groupTable, String onColumn) {
        context.getDbms()
                .execute(String.format("USE %s", groupSchema))
                .execute(String.format("CREATE TABLE %s AS SELECT %s, COUNT(*) AS group_size FROM %s.%s GROUP BY %s", groupTable, onColumn, originSchema, originTable, onColumn));
    }

    public void buildGroupSizeTable(String originSchema, String originTable, String groupView, String onColumn) {
        Dataset<Row> groupDF = context.getDbms()
                .execute(String.format("SELECT %s, COUNT(*) AS group_size FROM %s.%s GROUP BY %s", onColumn, originSchema, originTable, onColumn))
                .getResultSet()
                .cache();
        groupDF.createOrReplaceTempView(groupView);
        groupDF.count();
    }
}
