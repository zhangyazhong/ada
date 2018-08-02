package daslab.inspector;

import com.google.common.collect.Maps;
import daslab.bean.AdaBatch;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;
import daslab.utils.AdaTimer;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author zyz
 * @version 2018-05-14
 */
public class TableMeta {
    public static class MetaInfo {
        private TableColumn column;
        private double n;
        private double n_;
        private double s2;
        private long d;
        private int x;
        private double avg;
        private double sum;
        private double e;

        MetaInfo(TableColumn column) {
            this.column = column;
        }

        static MetaInfo calc(TableColumn column, double s2, long d, double sum, double e, double z) {
            MetaInfo metaInfo = new MetaInfo(column);
            metaInfo.s2 = s2;
            metaInfo.d = d;
            metaInfo.avg = sum / d;
            metaInfo.sum = sum;
            metaInfo.n_ = (z * z * metaInfo.s2) / (e * e);
            metaInfo.n = (metaInfo.n_ * metaInfo.d) / (metaInfo.n_ + metaInfo.d);
            if (metaInfo.n - Math.floor(metaInfo.n) < 0.000000001) {
                metaInfo.x = (int) (metaInfo.n - 1);
            } else {
                metaInfo.x = (int) Math.floor(metaInfo.n);
            }
            metaInfo.e = e;
            return metaInfo;
        }

        public double getN() {
            return n;
        }

        public double getN_() {
            return n_;
        }

        public double getS2() {
            return s2;
        }

        public long getD() {
            return d;
        }

        public int getX() {
            return x;
        }

        public double getSum() {
            return sum;
        }

        public double getAvg() {
            return avg;
        }

        public double getE() {
            return e;
        }

        @Override
        public String toString() {
            return String.format("{column: %s, n: %.2f, n^: %.2f, s2: %.2f, d: %d, x: %d, avg: %.2f, sum: %.2f, e: %.2f}",
                    column, n, n_, s2, d, x, avg, sum, e);
        }
    }

    private AdaContext context;
    private TableSchema tableSchema;
    private Map<TableColumn, MetaInfo> tableMetaMap;
    private long cardinality;
    private String metaClause;

    public TableMeta(AdaContext context, TableSchema tableSchema) {
        this.context = context;
        this.tableSchema = tableSchema;
        this.tableMetaMap = Maps.newHashMap();
    }

    public void init() {
        double confidence = Double.parseDouble(context.get("query.confidence_internal_"));
        this.metaClause = metaClause();
        String sql = String.format("SELECT %s FROM %s.%s", metaClause,
                tableSchema.getDbName(), tableSchema.getTableName());
        context.getDbmsSpark2().execute(sql);
        cardinality = context.getDbmsSpark2().getResultAsLong(0, "count");
        for (TableColumn column : tableSchema.getColumns()) {
            if (column.getColumnType().isInt() || column.getColumnType().isDouble()) {
                double var = context.getDbmsSpark2().getResultAsDouble(0, "var_pop_" + column.getColumnName());
                double sum = context.getDbmsSpark2().getResultAsLong(0, "sum_" + column.getColumnName());
                double avg = context.getDbmsSpark2().getResultAsDouble(0, "avg_" + column.getColumnName());
                double errorBound = avg * 0.3;
                MetaInfo metaInfo = MetaInfo.calc(column, var, cardinality, sum, errorBound, confidence);
                tableMetaMap.put(column, metaInfo);
                AdaLogger.debug(this, "Initially table meta[" + column.getColumnName() + "]: " + metaInfo.toString());
            }
        }
    }

    public TableMeta refresh(AdaBatch adaBatch) {
        double confidence = Double.parseDouble(context.get("query.confidence_internal_"));
        String sql = String.format("SELECT %s FROM %s.%s", metaClause,
                adaBatch.getDbName(), adaBatch.getTableName());
        context.getDbmsSpark2().execute(sql);
        int newCount = adaBatch.getSize();
        long totalCount = newCount + cardinality;
        Map<TableColumn, MetaInfo> batchMetaMap = Maps.newHashMap();
        for (TableColumn column : tableSchema.getColumns()) {
            if (column.getColumnType().isInt() || column.getColumnType().isDouble()) {
                double oldVar = tableMetaMap.get(column).getS2();
                double oldSum = tableMetaMap.get(column).getSum();
                double oldAvg = tableMetaMap.get(column).getAvg();
                double newVar = context.getDbmsSpark2().getResultAsDouble(0, "var_pop_" + column.getColumnName());
                double newSum = context.getDbmsSpark2().getResultAsLong(0, "sum_" + column.getColumnName());
                double newAvg = newSum / newCount;
                double totalSum = newSum + oldSum;
                double totalAvg = totalSum / totalCount;
                double totalVar = (cardinality * (oldVar + (totalAvg - oldAvg) * (totalAvg - oldAvg))
                        + newCount * (newVar + (totalAvg - newAvg) * (totalAvg - newAvg))) / totalCount;
                double errorBound = tableMetaMap.get(column).getE();
                batchMetaMap.put(column, MetaInfo.calc(column, totalVar, totalCount, totalSum, errorBound, confidence));
                AdaLogger.debug(this, "Batch[" + context.getBatchCount() + "] table meta[" + column.getColumnName() + "]: " + batchMetaMap.get(column).toString());
            }
        }
        cardinality += newCount;
        tableMetaMap = batchMetaMap;

        AdaLogger.info(this, String.format("Table[%s] cardinality: %d", tableSchema.getTableName(), cardinality));
        context.set(tableSchema.getTableName() + "_cardinality", String.valueOf(cardinality));

        return this;
    }

    private String metaClause() {
        StringBuilder selectClause = new StringBuilder(countClause("count")).append(", ");
        for (TableColumn column : tableSchema.getColumns()) {
            if (column.getColumnType().isInt() || column.getColumnType().isDouble()) {
                selectClause
                        .append(varClause(column, "var_pop_" + column.getColumnName())).append(", ")
                        .append(sumClause(column, "sum_" + column.getColumnName())).append(", ")
                        .append(avgClause(column, "avg_" + column.getColumnName())).append(", ");
            }
        }
        this.metaClause = StringUtils.substringBeforeLast(selectClause.toString(), ",");
        return this.metaClause;
    }

    public long getCardinality() {
        return cardinality;
    }

    public Map<TableColumn, MetaInfo> getTableMetaMap() {
        return tableMetaMap;
    }

    private String varClause(TableColumn column, String alias) {
        return String.format("var_pop(%s) as %s", column.getColumnName(), alias);
    }

    private String sumClause(TableColumn column, String alias) {
        return String.format("sum(%s) as %s", column.getColumnName(), alias);
    }

    private String avgClause(TableColumn column, String alias) {
        return String.format("avg(%s) as %s", column.getColumnName(), alias);
    }

    private String countClause(String alias) {
        return "count(*) as " + alias;
    }
}
