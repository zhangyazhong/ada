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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author zyz
 * @version 2018-05-11
 */
public class AdaContext {
    private final String CONFIG_FILE = "core.properties";

    private Map<String, String> configs;
    private SocketBatchReceiver socketReceiver;
    private LocalFileBatchReceiver fileReceiver;
    private HdfsBathReceiver hdfsReceiver;
//    private DbmsHive2 dbmsHive2;
    private DbmsSpark2 dbmsSpark2;
    private TableMeta tableMeta;
    private SamplingController samplingController;
    private int batchCount;
    private VerdictSpark2Context verdictSpark2Context;
    private List<ExecutionReport> executionReports;

    public AdaContext() {
        configs = Maps.newHashMap();
        updateConfigsFromPropertyFile(CONFIG_FILE);
        socketReceiver = SocketBatchReceiver.build(this);
        fileReceiver = LocalFileBatchReceiver.build(this);
        hdfsReceiver = HdfsBathReceiver.build(this);
//        dbmsHive2 = DbmsHive2.getInstance(this);
        dbmsSpark2 = DbmsSpark2.getInstance(this);
        tableMeta = new TableMeta(this, dbmsSpark2.desc());
        batchCount = 0;
        samplingController = new SamplingController(this);
        executionReports = Lists.newLinkedList();
        try {
            verdictSpark2Context = new VerdictSpark2Context(getDbms().getSparkSession().sparkContext());
        } catch (VerdictException e) {
            e.printStackTrace();
        }
    }

    private void updateConfigsFromPropertyFile(String configFile) {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(configFile);
        Properties properties = ConfigHandler.load(inputStream);
        for (String prop : properties.stringPropertyNames()) {
            String value = properties.getProperty(prop);
            set(prop, value);
        }
    }

    public void set(String key, String value) {
        configs.put(key, value);
    }

    public String get(String key) {
        return configs.get(key);
    }

    public void start() {
        tableMeta.init();
//        socketReceiver.start();
    }

    public void receive(File file) {
        fileReceiver.receive(file);
    }

    public ExecutionReport receive(String hdfsLocation) {
        createReport();
        hdfsReceiver.receive(hdfsLocation);
        return currentReport();
    }

    public void afterOneBatch(String batchLocation) {
        batchCount++;
        AdaBatch adaBatch = getDbmsSpark2().load(batchLocation);

        // REPORT: sampling.cost.total (start)
        Long startTime = System.currentTimeMillis();
        refreshSample();
        tableMeta.refresh(adaBatch);
        Map<Sample, Sampling> strategies = sampling(adaBatch);
        // REPORT: sampling.cost.total (stop)
        Long finishTime = System.currentTimeMillis();
        String samplingTime = String.format("%d:%02d.%03d", (finishTime - startTime) / 60000, ((finishTime - startTime) / 1000) % 60, (finishTime - startTime) % 1000);
        currentReport().put("sampling.cost.total", finishTime - startTime);

        AdaLogger.info(this, String.format("AdaBatch(%d) [%s] sampling time cost: %s ", adaBatch.getSize(), strategies.toString(), samplingTime));
        // REPORT: sampling.strategies
        writeIntoReport("sampling.strategies", strategies);
    }

//    public DbmsHive2 getDbmsHive2() {
//        return dbmsHive2;
//    }

    public Map<Sample, Sampling> sampling(AdaBatch adaBatch) {
        Map<Sample, SampleStatus> sampleStatusMap = samplingController.verify(tableMeta.getTableMetaMap(), tableMeta.getCardinality());
        AdaLogger.info(this, sampleStatusMap.toString());

        Map<Sample, Sampling> strategies = Maps.newHashMap();

        // REPORT: sampling.cost.sampling (start)
        AdaTimer timer = AdaTimer.create();
        sampleStatusMap.forEach((sample, status) -> {
            if (status.whetherResample()) {
                AdaLogger.info(this, String.format("Sample's[%s][%.2f] columns need to be updated: %s.",
                        sample.sampleType, sample.samplingRatio,
                        StringUtils.join(status.resampleColumns().stream().map(TableColumn::toString).toArray(), ", ")));
                AdaLogger.info(this, String.format("Use %s strategy to resample sample[%s][%.2f][%s].", getSamplingController().getResamplingStrategy().name(), sample.sampleType, sample.samplingRatio, sample.onColumn));
                getSamplingController().resample(sample, adaBatch, status.getMaxExpectedRatio(1.1));
                strategies.put(sample, Sampling.RESAMPLE);
            } else {
                AdaLogger.info(this, String.format("Sample's[%s][%.2f]: no column needs to be updated.",
                        sample.sampleType, sample.samplingRatio));
                AdaLogger.info(this, String.format("Use %s strategy to update sample[%s][%.2f][%s].", getSamplingController().getSamplingStrategy().name(), sample.sampleType, sample.samplingRatio, sample.onColumn));
                getSamplingController().update(sample, adaBatch);
                strategies.put(sample, Sampling.UPDATE);
            }
        });
        // REPORT: sampling.cost.sampling (stop)
        writeIntoReport("sampling.cost.sampling", String.valueOf(timer.stop()));

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
}
