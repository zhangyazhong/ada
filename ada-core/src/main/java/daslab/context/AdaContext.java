package daslab.context;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.bean.*;
import daslab.inspector.TableColumn;
import daslab.inspector.TableMeta;
import daslab.sampling.SamplingController;
import daslab.server.HdfsBathReceiver;
import daslab.server.LocalFileBatchReceiver;
import daslab.server.SocketBatchReceiver;
import daslab.utils.AdaLogger;
import daslab.utils.AdaTimer;
import daslab.utils.ConfigHandler;
import daslab.warehouse.DbmsSpark2;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author zyz
 * @version 2018-05-11
 */
public class AdaContext {
    private final String[] CONFIGS = {"classpath: core.properties", "/tmp/ada/config/ada_core.properties"};

    private Map<String, String> configs;
    private SocketBatchReceiver socketReceiver;
    private LocalFileBatchReceiver fileReceiver;
    private HdfsBathReceiver hdfsReceiver;
    private DbmsSpark2 dbmsSpark2;
    private TableMeta tableMeta;
    private SamplingController samplingController;
    private Map<Sample, SampleStatus> sampleStatusMap;
    private int batchCount;
    private VerdictSpark2Context verdictSpark2Context;
    private List<ExecutionReport> executionReports;
    private Map<Sample, Sampling> strategies;
    private boolean forceResample;
    private boolean skipSampling;
    private boolean enableAdaptive;

    public AdaContext() {
        configs = Maps.newHashMap();
        updateConfigsFromPropertyFile(CONFIGS);
        socketReceiver = SocketBatchReceiver.build(this);
        fileReceiver = LocalFileBatchReceiver.build(this);
        hdfsReceiver = HdfsBathReceiver.build(this);
        dbmsSpark2 = DbmsSpark2.getInstance(this);
        tableMeta = new TableMeta(this, dbmsSpark2.desc());
        batchCount = 0;
        forceResample = false;
        skipSampling = false;
        enableAdaptive = true;
        samplingController = new SamplingController(this);
        executionReports = Lists.newLinkedList();
        strategies = Maps.newHashMap();
        try {
            verdictSpark2Context = new VerdictSpark2Context(getDbms().getSparkSession().sparkContext());
        } catch (VerdictException e) {
            e.printStackTrace();
        }
    }

    private void updateConfigsFromPropertyFile(String... CONFIGS) {
        for (String config : CONFIGS) {
            try {
                InputStream inputStream;
                if (config.startsWith("classpath")) {
                    inputStream = this.getClass().getClassLoader().getResourceAsStream(StringUtils.substringAfter(config, "classpath:").trim());
                } else {
                    File file = new File(config);
                    if (!file.exists()) {
                        continue;
                    }
                    inputStream = new FileInputStream(config);
                }
                Properties properties = ConfigHandler.load(inputStream);
                for (String prop : properties.stringPropertyNames()) {
                    String value = properties.getProperty(prop).trim();
                    set(prop, value);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public void set(String key, String value) {
        configs.put(key, value);
    }

    public String get(String key) {
        return configs.get(key);
    }

    public AdaContext start() {
        tableMeta.init();
        return this;
    }

    public AdaContext start(boolean forceResample) {
        this.forceResample = forceResample;
        return start();
    }

    public AdaContext skipSampling(boolean skipSampling) {
        this.skipSampling = skipSampling;
        return this;
    }

    public AdaContext enableAdaptive(boolean enableAdaptive) {
        this.enableAdaptive = enableAdaptive;
        return this;
    }

    public void receive(File file) {
        fileReceiver.receive(file);
    }

    public ExecutionReport receive(String hdfsLocation) {
        printBlankLine(5);
        createReport();
        hdfsReceiver.receive(hdfsLocation);
        return currentReport();
    }

    public ExecutionReport receive(String[] hdfsLocations) {
        printBlankLine(5);
        createReport();
        hdfsReceiver.receive(hdfsLocations);
        return currentReport();
    }

    public void afterOneBatch(String... batchLocations) {
        AdaBatch adaBatch = batchLocations.length > 1 ? getDbmsSpark2().load(batchLocations) : getDbmsSpark2().load(batchLocations[0]);

        // REPORT: sampling.cost.total (start)
        Long startTime = System.currentTimeMillis();
        // REPORT: sampling.cost.pre-process (start)
        AdaTimer timer = AdaTimer.create();
        tableMeta.refresh(adaBatch);
        // REPORT: sampling.cost.pre-process (stop)
        writeIntoReport("sampling.cost.pre-process", timer.stop());

        if (!skipSampling) {
            // REPORT: sampling.cost.sampling (start)
            timer = AdaTimer.create();
            strategies = sampling(adaBatch);
            // REPORT: sampling.strategies
            writeIntoReport("sampling.strategies", strategies);
            // REPORT: sampling.cost.sampling (stop)
            writeIntoReport("sampling.cost.sampling", String.valueOf(timer.stop()));
        }

        // REPORT: sampling.cost.total (stop)
        Long finishTime = System.currentTimeMillis();
        String samplingTime = AdaTimer.format(finishTime - startTime);
        currentReport().put("sampling.cost.total", finishTime - startTime);

        AdaLogger.info(this, String.format("AdaBatch(%d) [%s] sampling time cost: %s ", adaBatch.getSize(), strategies.toString(), samplingTime));
    }

    public Map<Sample, Sampling> sampling(AdaBatch adaBatch) {
        sampleStatusMap = samplingController.verify(tableMeta.getTableMetaMap(), tableMeta.getCardinality());
        AdaLogger.info(this, sampleStatusMap.toString());

        Map<Sample, Sampling> strategies = Maps.newHashMap();
        sampleStatusMap.forEach((sample, status) -> {
            // REPORT: sampling.origin.{sample.brief}
            writeIntoReport("sample.origin." + sample.brief(), sample.sampleSize);
            // REPORT: sampling.needed.{sample.brief}
            writeIntoReport("sample.needed." + sample.brief(), status.whetherResample() ? (long) (status.getMaxExpectedSize() * 1.1) : status.getMaxExpectedSize());
            if (status.whetherResample() || forceResample) {
                if (!forceResample && enableAdaptive && status.M() <= adaBatch.getSize()) {
                    AdaLogger.info(this, String.format("Sample's[%s][%.2f] columns need to be adaptive: %s.",
                            sample.sampleType, sample.samplingRatio,
                            StringUtils.join(status.resampleColumns().stream().map(TableColumn::toString).toArray(), ", ")));
                    AdaLogger.info(this, String.format("Use %s strategy to adaptive sample[%s][%.2f][%s].",
                            getSamplingController().getAdaptiveStrategy().name(), sample.sampleType, sample.samplingRatio, sample.onColumn));
                    getSamplingController().adaptive(sample, adaBatch);
                    strategies.put(sample, Sampling.ADAPATIVE);
                } else {
                    AdaLogger.info(this, String.format("Sample's[%s][%.2f] columns need to be resample: %s.",
                            sample.sampleType, sample.samplingRatio,
                            StringUtils.join(status.resampleColumns().stream().map(TableColumn::toString).toArray(), ", ")));
                    AdaLogger.info(this, String.format("Use %s strategy to resample sample[%s][%.2f][%s].",
                            getSamplingController().getResamplingStrategy().name(), sample.sampleType, sample.samplingRatio, sample.onColumn));
                    getSamplingController().resample(sample, adaBatch, status.getMaxExpectedRatio(1.1));
                    strategies.put(sample, Sampling.RESAMPLE);
                }
            } else {
                AdaLogger.info(this, String.format("Sample's[%s][%.2f]: no column needs to be resample.",
                        sample.sampleType, sample.samplingRatio));
                AdaLogger.info(this, String.format("Use %s strategy to update sample[%s][%.2f][%s].",
                        getSamplingController().getSamplingStrategy().name(), sample.sampleType, sample.samplingRatio, sample.onColumn));
                getSamplingController().update(sample, adaBatch);
                strategies.put(sample, Sampling.UPDATE);
            }
        });
        return strategies;
    }

    public VerdictSpark2Context getVerdict() {
        return verdictSpark2Context;
    }

    public DbmsSpark2 getDbms() {
        return getDbmsSpark2();
    }

    public DbmsSpark2 getDbmsSpark2() {
        return dbmsSpark2;
    }

    public SamplingController getSamplingController() {
        return samplingController;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public int increaseBatchCount() {
        batchCount++;
        return batchCount;
    }

    private void createReport() {
        executionReports.add(new ExecutionReport());
    }

    private ExecutionReport currentReport() {
        return executionReports.get(executionReports.size() - 1);
    }

    public AdaContext writeIntoReport(String key, Object value) {
        currentReport().put(key, value);
        return this;
    }

    public void refreshSample() {
        getSamplingController().getSamplingStrategy().getSamples(true).forEach(sample -> AdaLogger.info(this, "Ada Current Sample - " + sample.toString()));
    }

    public TableMeta getTableMeta() {
        return tableMeta;
    }

    public SampleStatus getSampleStatus(Sample sample) {
        return sampleStatusMap.get(sample);
    }

    public void printBlankLine(int number) {
        while (number > 0) {
            number--;
            AdaLogger.info(this, "");
        }
    }
}
