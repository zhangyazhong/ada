package daslab.bean;

import com.google.common.collect.Lists;
import daslab.inspector.TableColumn;
import jersey.repackaged.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class SampleStatus {
    private Sample sample;
    private long tableSize;
    private Map<TableColumn, Long> expectedSizes;

    public SampleStatus(Sample sample, long tableSize) {
        this.sample = sample;
        this.tableSize = tableSize;
        expectedSizes = Maps.newHashMap();
    }

    public void push(TableColumn column, Long expectedSize) {
        expectedSizes.put(column, expectedSize);
    }

    public long getMaxExpectedSize() {
        return expectedSizes.values().stream().reduce(Math::max).orElse(0L);
    }

    public double getMaxExpectedRatio() {
        return getMaxExpectedRatio(1.0);
    }

    public double getMaxExpectedRatio(double scale) {
        return scale * getMaxExpectedSize() / tableSize;
    }

    public int getMaxExpectedRatioInSQL() {
        return getMaxExpectedRatioInSQL(1.0);
    }

    public int getMaxExpectedRatioInSQL(double scale) {
        double ratio = getMaxExpectedRatio() * scale;
        return Math.min((int) Math.round(ratio * 100), 100);
    }

    public boolean whetherResample() {
        return getMaxExpectedSize() > sample.sampleSize;
    }

    public List<TableColumn> resampleColumns() {
        List<TableColumn> columns = Lists.newArrayList();
        expectedSizes.forEach((column, size) -> {
            if (size > sample.sampleSize) {
                columns.add(column);
            }
        });
        return columns;
    }

    public long M() {
        return 10 * Math.max(getMaxExpectedSize() - sample.sampleSize, 0);
    }

    @Override
    public String toString() {
        StringBuilder status = new StringBuilder("{");
        expectedSizes.forEach((column, size) -> status.append("(").append(column).append(", ").append(size).append(")"));
        status.append("}");
        return status.toString();
    }
}
