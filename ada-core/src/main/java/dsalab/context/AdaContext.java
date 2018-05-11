package dsalab.context;

import com.google.common.collect.Maps;
import dsalab.server.BatchReceiver;
import dsalab.utils.ConfigHandler;

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

    public AdaContext() {
        configs = Maps.newHashMap();
        updateConfigsFromPropertyFile(CONFIG_FILE);
        receiver = BatchReceiver.build(this);
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
        receiver.start();
    }
}
