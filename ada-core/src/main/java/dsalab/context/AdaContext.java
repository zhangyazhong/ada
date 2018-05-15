package dsalab.context;

import com.google.common.collect.Maps;
import dsalab.inspector.TableMeta;
import dsalab.sampling.SamplingController;
import dsalab.server.BatchReceiver;
import dsalab.utils.ConfigHandler;
import dsalab.warehouse.DbmsHive2;
import dsalab.warehouse.DbmsSpark2;

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
    private DbmsHive2 dbmsHive2;
    private DbmsSpark2 dbmsSpark2;
    private TableMeta tableMeta;
    private SamplingController samplingController;

    public AdaContext() {
        configs = Maps.newHashMap();
        updateConfigsFromPropertyFile(CONFIG_FILE);
        receiver = BatchReceiver.build(this);
        dbmsHive2 = DbmsHive2.getInstance(this);
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
        dbmsHive2.load(batchLocation);
    }

    public DbmsHive2 getDbmsHive2() {
        return dbmsHive2;
    }

    public DbmsSpark2 getDbmsSpark2() {
        return dbmsSpark2;
    }

    public SamplingController getSamplingController() {
        return samplingController;
    }
}
