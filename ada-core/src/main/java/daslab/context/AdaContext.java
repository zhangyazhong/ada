package daslab.context;

import com.google.common.collect.Maps;
import daslab.bean.Batch;
import daslab.bean.Sampling;
import daslab.inspector.TableMeta;
import daslab.sampling.SamplingController;
import daslab.server.BatchReceiver;
import daslab.utils.AdaLogger;
import daslab.utils.ConfigHandler;
import daslab.warehouse.DbmsSpark2;

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
    private BatchReceiver receiver;
//    private DbmsHive2 dbmsHive2;
    private DbmsSpark2 dbmsSpark2;
    private TableMeta tableMeta;
    private SamplingController samplingController;

    public AdaContext() {
        configs = Maps.newHashMap();
        updateConfigsFromPropertyFile(CONFIG_FILE);
        receiver = BatchReceiver.build(this);
//        dbmsHive2 = DbmsHive2.getInstance(this);
        dbmsSpark2 = DbmsSpark2.getInstance(this);
        tableMeta = new TableMeta(this, dbmsSpark2.desc());
        samplingController = new SamplingController(this);
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
        receiver.start();
    }

    public void afterOneBatch(String batchLocation) {
        Batch batch = getDbmsSpark2().load(batchLocation);
        Long startTime = System.currentTimeMillis();
        Sampling strategy = tableMeta.refresh(batch);
        Long finishTime = System.currentTimeMillis();
        String samplingTime = String.format("%d:%02d.%03d", (finishTime - startTime) / 60000, ((finishTime - startTime) / 1000) % 60, (finishTime - startTime) % 1000);

        AdaLogger.info(this, String.format("Batch(%d) [%s] sampling time cost: %s ", batch.getSize(), strategy.toString(), samplingTime));
    }

//    public DbmsHive2 getDbmsHive2() {
//        return dbmsHive2;
//    }

    public DbmsSpark2 getDbmsSpark2() {
        return dbmsSpark2;
    }

    public SamplingController getSamplingController() {
        return samplingController;
    }
}
