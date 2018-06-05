package daslab.inspector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.bean.Batch;
import daslab.bean.Sampling;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author zyz
 * @version 2018-05-14
 */
public class TableMeta {
    private AdaContext context;
    private TableSchema tableSchema;
    private Map<TableColumn, MetaInfo> tableMetaMap;
    private int cardinality;
    private String metaClause;

    public TableMeta(AdaContext context, TableSchema tableSchema) {
        this.context = context;
        this.tableSchema = tableSchema;
        this.tableMetaMap = Maps.newHashMap();
    }

    public void init() {
        double errorBound = Double.parseDouble(context.get("query.error_bound"));
        double confidence = Double.parseDouble(context.get("query.confidence_internal_"));
        this.metaClause = metaClause();
        String sql = String.format("SELECT %s FROM %s.%s", metaClause,
                tableSchema.getDbName(), tableSchema.getTableName());
        context.getDbmsSpark2().execute(sql);
        cardinality = (int) context.getDbmsSpark2().getResultAsLong(0, "count");
        for (TableColumn column : tableSchema.getColumns()) {
            if (column.getColumnType().isInt() || column.getColumnType().isDouble()) {
                double var = context.getDbmsSpark2().getResultAsDouble(0, "var_pop_" + column.getColumnName());
                double sum = context.getDbmsSpark2().getResultAsLong(0, "sum_" + column.getColumnName());
                MetaInfo metaInfo = MetaInfo.calc(column, var, cardinality, sum, errorBound, confidence);
                tableMetaMap.put(column, metaInfo);
            }
        }
    }

    public Sampling refresh(Batch batch) {
        double errorBound = Double.parseDouble(context.get("query.error_bound"));
        double confidence = Double.parseDouble(context.get("query.confidence_internal_"));
        String sql = String.format("SELECT %s FROM %s.%s", metaClause,
                batch.getDbName(), batch.getTableName());
        context.getDbmsSpark2().execute(sql);
        int newCount = batch.getSize();
//        int totalCount = newCount + ((MetaInfo[]) tableMetaMap.values().toArray())[0].getD();
        int totalCount = newCount + cardinality;
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
                batchMetaMap.put(column, MetaInfo.calc(column, totalVar, totalCount, totalSum, errorBound, confidence));
            }
        }

        List<TableColumn> illegalColumns = verify(batch, batchMetaMap);
        cardinality += newCount;

        if (illegalColumns.size() > 0) {
            AdaLogger.info(this, String.format("Columns need to be updated: %s.",
                    StringUtils.join(illegalColumns.stream().map(TableColumn::toString).toArray(), ", ")));
            AdaLogger.info(this, "Use " + context.getSamplingController().getResamplingStrategy().strategyName() + " strategy to resample.");
            context.getSamplingController().resample(batch);
            return Sampling.RESAMPLE;
        } else {
            AdaLogger.info(this, "No column needs to be updated.");
            AdaLogger.info(this, "Use " + context.getSamplingController().getSamplingStrategy().strategyName() + " strategy to update sample.");
            context.getSamplingController().update(batch);
            return Sampling.UPDATE;
        }
    }

    private List<TableColumn> verify(Batch batch, Map<TableColumn, MetaInfo> batchMetaMap) {
        double errorBound = Double.parseDouble(context.get("query.error_bound"));
        double confidence = Double.parseDouble(context.get("query.confidence_internal_"));
        List<TableColumn> illegalColumns = Lists.newArrayList();
        for (TableColumn column : tableMetaMap.keySet()) {
            MetaInfo tableMetaInfo = tableMetaMap.get(column);
            MetaInfo batchMetaInfo = batchMetaMap.get(column);

            double x = tableMetaInfo.getX();
            double deltaS2 = batchMetaInfo.getS2() - tableMetaInfo.getS2();
            double e2 = Math.pow(errorBound, 2);
            double z2 = Math.pow(confidence, 2);
            double nt = Math.ceil(tableMetaInfo.getN());

            boolean flag = false;
            // judgement 1
            if (batchMetaInfo.getN_() > nt
                    && tableMetaInfo.getD() * x > batchMetaInfo.getN_() * (tableMetaInfo.getD() - x)
                    && batch.getSize() > (e2 * tableMetaInfo.getD() * x - z2 * (tableMetaInfo.getS2() + deltaS2) * (tableMetaInfo.getD() - x)) / (tableMetaInfo.getS2() * z2 - e2 * x + z2 * deltaS2)
                    && batch.getSize() <= (e2 * tableMetaInfo.getD() * nt - z2 * (tableMetaInfo.getS2() + deltaS2) * (tableMetaInfo.getD() - nt)) / (tableMetaInfo.getS2() * z2 - e2 *  nt + z2 * deltaS2)) {
                flag = true;
            }
            // judgement 2
            if (batchMetaInfo.getN_() > nt
                    && tableMetaInfo.getD() * nt > batchMetaInfo.getN_() * (tableMetaInfo.getD() - nt)
                    && tableMetaInfo.getD() * x < batchMetaInfo.getN_() * (tableMetaInfo.getD() - x)
                    && batch.getSize() > 0
                    && batch.getSize() <= (e2 * tableMetaInfo.getD() * nt - z2 * (tableMetaInfo.getS2() + deltaS2) * (tableMetaInfo.getD() - nt)) / (tableMetaInfo.getS2() * z2 - e2 * nt + z2 * deltaS2)) {
                flag = true;
            }
            // judgement 3
            if (x < batchMetaInfo.getN_()
                    && batchMetaInfo.getN_() < nt
                    && tableMetaInfo.getD() * x > batchMetaInfo.getN_() * (tableMetaInfo.getD() - x)
                    && batch.getSize() > (e2 * tableMetaInfo.getD() * x - z2 * (tableMetaInfo.getS2() + deltaS2) * (tableMetaInfo.getD() - x)) / (tableMetaInfo.getS2() * z2 - e2 * x + z2 * deltaS2)) {
                flag = true;
            }
            // judgement 4
            if (x < batchMetaInfo.getN_()
                    && batchMetaInfo.getN_() < nt
                    && tableMetaInfo.getD() * x > batchMetaInfo.getN_() * (tableMetaInfo.getD() - x)
                    && batch.getSize() > 0) {
                flag = true;
            }

            if (!flag) {
                illegalColumns.add(column);
            }
        }
        return illegalColumns;
    }

    private String metaClause() {
        StringBuilder selectClause = new StringBuilder(countClause("count")).append(", ");
        for (TableColumn column : tableSchema.getColumns()) {
            if (column.getColumnType().isInt() || column.getColumnType().isDouble()) {
                selectClause
                        .append(varClause(column, "var_pop_" + column.getColumnName())).append(", ")
                        .append(sumClause(column, "sum_" + column.getColumnName())).append(", ");
            }
        }
        this.metaClause = StringUtils.substringBeforeLast(selectClause.toString(), ",");
        return this.metaClause;
    }

    private String varClause(TableColumn column, String alias) {
        return String.format("var_pop(%s) as %s", column.getColumnName(), alias);
    }

    private String sumClause(TableColumn column, String alias) {
        return String.format("sum(%s) as %s", column.getColumnName(), alias);
    }

    private String countClause(String alias) {
        return "count(*) as " + alias;
    }
}

class MetaInfo {
    private TableColumn column;
    private double n;
    private double n_;
    private double s2;
    private int d;
    private int x;
    private double avg;
    private double sum;

    public MetaInfo(TableColumn column) {
        this.column = column;
    }

    public static MetaInfo calc(TableColumn column, double s2, int d, double sum, double e, double z) {
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

    public int getD() {
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
}
