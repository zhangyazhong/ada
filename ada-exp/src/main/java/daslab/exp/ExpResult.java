package daslab.exp;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExpResult {
    private List<String> header;
    private Map<String, List<String>> results;

    public ExpResult() {
        this.results = Maps.newLinkedHashMap();
    }

    public ExpResult(List<String> header) {
        this();
        setHeader(header);
    }

    public void setHeader(List<String> header) {
        this.header = header;
    }

    public void setHeader(String... names) {
        header = Lists.newLinkedList();
        header.addAll(Arrays.asList(names));
    }

    public List<String> getHeader() {
        return header;
    }

    public void setResults(Map<String, List<String>> results) {
        this.results = results;
    }

    public void addResult(String key, List<String> results) {
        this.results.put(key, results);
    }

    public void addResult(String key, String result) {
        this.results.computeIfAbsent(key, k -> Lists.newLinkedList());
        results.get(key).add(result);
    }

    public List<String> getRowKeys() {
        List<String> rowKeys = Lists.newLinkedList();
        results.forEach((key, value) -> rowKeys.add(key));
        return rowKeys;
    }

    public List<String> getColumns(String rowKey) {
        return results.get(rowKey);
    }
}
