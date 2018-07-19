package daslab.bean;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;

public class ExecutionReport implements Serializable {
    private Map<String, Object> report;

    public ExecutionReport() {
        report = Maps.newConcurrentMap();
    }

    public void put(String key, Object value) {
        report.put(key, value);
    }

    public Object get(String key) {
        return report.get(key);
    }

    public String getString(String key) {
        return report.get(key).toString();
    }

    public Long getLong(String key) {
        return (Long) report.get(key);
    }
}
