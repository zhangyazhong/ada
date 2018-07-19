package daslab.exp;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

    public void save(String path) {
        File file = new File(path);
        file.getParentFile().mkdirs();
        try {
            FileWriter fileWriter = new FileWriter(file);
            String header = StringUtils.join(this.getHeader().toArray(), ",");
            fileWriter.write(header + "\r\n");
            final StringBuilder content = new StringBuilder();
            this.getRowKeys().forEach(key -> content.append(key).append(",").append(StringUtils.join(this.getColumns(key).toArray(), ",")).append("\r\n"));
            fileWriter.write(content.toString());
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
