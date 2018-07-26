package daslab.bean;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;
import java.util.regex.Pattern;

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

    public Map<String, Object> search(String key) {
        Map<String, Object> part = Maps.newConcurrentMap();
        if (report.containsKey(key)) {
            part.put(key, get(key));
        }
        String reg = key + "\\..*";
        Pattern pattern = Pattern.compile(reg);
        report.keySet().forEach(_key -> {
            if (pattern.matcher(_key).matches()) {
                part.put(_key, get(_key));
            }
        });
        return part;
    }
}
