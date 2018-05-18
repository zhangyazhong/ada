package daslab.context;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.bean.Batch;
import daslab.engine.BatchSender;
import daslab.source.DataSource;
import daslab.source.DataSourceManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
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
    private BatchSender batchSender;
    private List<Batch> bufferBatches;

    public ProducerContext() {
        configs = Maps.newHashMap();
        bufferBatches = Lists.newArrayList();
        updateConfigsFromPropertyFile(CONFIG_FILE);
        dataSource = DataSourceManager.build(this);
        batchSender = BatchSender.build(this);
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

    public Batch next() {
        bufferBatches.add(dataSource.next());
        return bufferBatches.get(bufferBatches.size() - 1);
    }

    public Batch pop() {
        Batch batch = bufferBatches.get(0);
        bufferBatches.remove(0);
        return batch;
    }

    public void mark() {
        dataSource.archive();
    }

    public void start() {
        batchSender.start();;
    }
}
