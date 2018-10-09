package daslab.exp;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class ExpResult {
    public final static String SEPARATOR = "|";
    private final static String SEPARATOR_REGEX = "\\|";
    private final static String NEW_LINE = "\r\n";

    private List<String> header;
    private Map<String, List<String>> results;

    public ExpResult() {
        this.results = Maps.newLinkedHashMap();
        this.header = Lists.newLinkedList();
    }

    public ExpResult(List<String> header) {
        this();
        setHeader(header);
    }

    public ExpResult(String... names) {
        this();
        addHeader(names);
    }

    public void setHeader(List<String> header) {
        this.header = header;
    }

    public void setHeader(String... names) {
        header = Lists.newLinkedList();
        header.addAll(Arrays.asList(names));
    }

    public void addHeader(String... names) {
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

    public void push(String key, String result) {
        addResult(key, result);
    }
    public ExpResult push(String key, String column, String result) {
        if (findColumnPosition(column) < 0) {
            addHeader(column);
        }
        int position = findColumnPosition(column);
        this.results.computeIfAbsent(key, k -> Lists.newLinkedList());
        for (int i = results.get(key).size() - 1; i < position; i++) {
            this.results.get(key).add("");
        }
        results.get(key).set(position, result);
        return this;
    }

    public String getCell(String time, String column) {
        return findColumnPosition(column) > 0 && results.get(time) != null ? results.get(time).get(findColumnPosition(column)) : null;
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
            String header = StringUtils.join(this.getHeader().toArray(), SEPARATOR);
            fileWriter.write(header + NEW_LINE);
            final StringBuilder content = new StringBuilder();
            this.getRowKeys().forEach(key -> {
                content.append(key).append(SEPARATOR).append(StringUtils.join(this.getColumns(key).toArray(), SEPARATOR));
                for (int i = this.getColumns(key).size(); i < this.header.size() - 1; i++) {
                    content.append(SEPARATOR);
                }
                content.append(NEW_LINE);
            });
            fileWriter.write(content.toString());
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void save(ExpPersistent expPersistent) {
        save(expPersistent.outputPath());
    }

    public static ExpResult load(String path) {
        try {
            Scanner scanner = new Scanner(new File(path));
            ExpResult expResult = new ExpResult(scanner.nextLine().split(SEPARATOR_REGEX));
            while (scanner.hasNextLine()) {
                String row = scanner.nextLine();
                if (row.length() < 1) {
                    break;
                }
                String[] cells = row.split(SEPARATOR_REGEX);
                for (int i = 1; i < cells.length; i++) {
                    expResult.push(cells[0], cells[i]);
                }
            }
            return expResult;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int findColumnPosition(String column) {
        int position = -1;
        for (int i = 1; i < header.size(); i++) {
            if (header.get(i).equals(column)) {
                position = i - 1;
            }
        }
        return position;
    }
}
