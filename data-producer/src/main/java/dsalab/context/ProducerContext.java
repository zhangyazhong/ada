package dsalab.context;

import com.google.common.collect.Maps;
import dsalab.engine.BatchEngine;
import dsalab.source.DataSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * @author zyz
 * @version 2018-05-09
 */
public class ProducerContext {
    private final String CONFIG_FILE = "producer.properties";

    private Map<String, String> configs;
    private DataSource dataSource;
    private BatchEngine batchEngine;

    public ProducerContext() {
        configs = Maps.newHashMap();
        updateConfigsFromPropertyFile(CONFIG_FILE);
    }

    private void updateConfigsFromPropertyFile(String configFile) {
        try {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(configFile);
            if (inputStream == null) {
                return;
            }
            Properties properties = new Properties();
            properties.load(inputStream);
            inputStream.close();
            for (String prop : properties.stringPropertyNames()) {
                String value = properties.getProperty(prop);
                set(prop, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void set(String key, String value) {
        configs.put(key, value);
    }

    public String get(String key) {
        return configs.get(key);
    }
}
