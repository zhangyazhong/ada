package daslab.context;

import com.google.common.collect.Maps;
import daslab.bean.AdaBatch;
import daslab.bean.Sample;
import daslab.bean.Sampling;
import daslab.inspector.TableMeta;
import daslab.sampling.SamplingController;
import daslab.server.HdfsBathReceiver;
import daslab.server.LocalFileBatchReceiver;
import daslab.server.SocketBatchReceiver;
import daslab.utils.AdaLogger;
import daslab.utils.ConfigHandler;
import daslab.warehouse.DbmsSpark2;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
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
    private FileWriter costWriter;
    private int batchCount;
    private VerdictSpark2Context verdictSpark2Context;

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
        try {
            verdictSpark2Context = new VerdictSpark2Context(getDbms().getSparkSession().sparkContext());
        } catch (VerdictException e) {
            e.printStackTrace();
        }
        try {
            costWriter = new FileWriter(new File(get("update.cost.path")));
            costWriter.write("batch,strategy,cost\r\n");
        } catch (IOException e) {
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

    private void set(String key, String value) {
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

    public void receive(String hdfsLocation) {
        hdfsReceiver.receive(hdfsLocation);
    }

    public void afterOneBatch(String batchLocation) {
        batchCount++;
        AdaBatch adaBatch = getDbmsSpark2().load(batchLocation);

        /*
        Long startTime = System.currentTimeMillis();
        Sampling strategy = tableMeta.refresh(adaBatch);
        Long finishTime = System.currentTimeMillis();
        String samplingTime = String.format("%d:%02d.%03d", (finishTime - startTime) / 60000, ((finishTime - startTime) / 1000) % 60, (finishTime - startTime) % 1000);

        try {
            costWriter.write(String.format("%d,%s,%s\r\n", batchCount, strategy.toString(), samplingTime));
        } catch (IOException e) {
            e.printStackTrace();
        }
        AdaLogger.info(this, String.format("AdaBatch(%d) [%s] sampling time cost: %s ", adaBatch.getSize(), strategy.toString(), samplingTime));
        */

        getSamplingController().getSamplingStrategy().getSamples().forEach(sample -> AdaLogger.info("Ada Current Sample - " + sample.toString()));

        Long startTime = System.currentTimeMillis();
        Map<Sample, Sampling> strategies = tableMeta.refresh(adaBatch);
        Long finishTime = System.currentTimeMillis();
        String samplingTime = String.format("%d:%02d.%03d", (finishTime - startTime) / 60000, ((finishTime - startTime) / 1000) % 60, (finishTime - startTime) % 1000);

        try {
            costWriter.write(String.format("%d,%s,%s\r\n", batchCount, strategies.toString(), samplingTime));
        } catch (IOException e) {
            e.printStackTrace();
        }
        AdaLogger.info(this, String.format("AdaBatch(%d) [%s] sampling time cost: %s ", adaBatch.getSize(), strategies.toString(), samplingTime));
    }

//    public DbmsHive2 getDbmsHive2() {
//        return dbmsHive2;
//    }

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
}
