package daslab.inspector;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import daslab.bean.AdaBatch;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

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

        public void setN(double samplesize) { this.n = samplesize; }

        public void setX(int samplesize) { this.x = samplesize; }

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
    private Set<TableColumn> inspectingColumns;

    public TableMeta(AdaContext context, TableSchema tableSchema) {
        this.context = context;
        this.tableSchema = tableSchema;
        this.tableMetaMap = Maps.newHashMap();
        if (context.get("dbms.table.meta.columns") != null) {
            this.inspectingColumns = Sets.newHashSet();
            String[] inspectingColumns = context.get("dbms.table.meta.columns").split(",");
            for (TableColumn tableColumn : tableSchema.getColumns()) {
                for (String inspectingColumn : inspectingColumns) {
                    if (tableColumn.getColumnName().equals(inspectingColumn.trim())) {
                        this.inspectingColumns.add(tableColumn);
                        break;
                    }
                }
            }
        } else {
            this.inspectingColumns = Sets.newHashSet(tableSchema.getColumns());
        }
    }

    public void init() {
        double confidence = Double.parseDouble(context.get("query.confidence_internal_"));
        this.metaClause = metaClause();
        String sql = String.format("SELECT %s FROM %s.%s", metaClause,
                tableSchema.getDbName(), tableSchema.getTableName());
        context.getDbmsSpark2().execute(sql);
        cardinality = context.getDbmsSpark2().getResultAsLong(0, "count");
        for (TableColumn column : tableSchema.getColumns()) {
            if (inspectingColumns.contains(column) && (column.getColumnType().isInt() || column.getColumnType().isDouble())) {
                double var = context.getDbmsSpark2().getResultAsDouble(0, "var_pop_" + column.getColumnName());
                double sum = column.getColumnType().isDouble() ?
                        context.getDbmsSpark2().getResultAsDouble(0, "sum_" + column.getColumnName()) :
                        context.getDbmsSpark2().getResultAsLong(0, "sum_" + column.getColumnName());
                double avg = context.getDbmsSpark2().getResultAsDouble(0, "avg_" + column.getColumnName());
                // double errorBound = avg * Double.parseDouble(context.get("query.error_bound"));
                double errorBound = context.get("query.error_bound_abs") != null ?
                        Double.parseDouble(context.get("query.error_bound_abs")) :
                        Math.min(avg * Double.parseDouble(context.get("query.error_bound")), 100000.0);
                MetaInfo metaInfo = MetaInfo.calc(column, var, cardinality, sum, errorBound, confidence);
                tableMetaMap.put(column, metaInfo);
                AdaLogger.debug(this, "Initially table meta[" + column.getColumnName() + "]: " + metaInfo.toString());
            }
        }
    }

    /**
     * updated by zhb
     * 2019-05-18
     **/
    public TableMeta refresh(AdaBatch adaBatch) {
        double confidence = Double.parseDouble(context.get("query.confidence_internal_"));

        // global statistical
        String sql = String.format("SELECT %s FROM %s.%s", metaClause,
                adaBatch.getDbName(), adaBatch.getTableName());
        context.getDbmsSpark2().execute(sql);
        int newCount = adaBatch.getSize();
        long totalCount = newCount + cardinality;
        Map<TableColumn, MetaInfo> batchMetaMap = Maps.newHashMap();
        for (TableColumn column : tableSchema.getColumns()) {
            if (inspectingColumns.contains(column) && (column.getColumnType().isInt() || column.getColumnType().isDouble())) {
                double oldVar = tableMetaMap.get(column).getS2();
                double oldSum = tableMetaMap.get(column).getSum();
                double oldAvg = tableMetaMap.get(column).getAvg();
                double newVar = context.getDbmsSpark2().getResultAsDouble(0, "var_pop_" + column.getColumnName());
                double newSum = column.getColumnType().isDouble() ?
                        context.getDbmsSpark2().getResultAsDouble(0, "sum_" + column.getColumnName()) :
                        context.getDbmsSpark2().getResultAsLong(0, "sum_" + column.getColumnName());
                double newAvg = newSum / newCount;
                double totalSum = newSum + oldSum;
                double totalAvg = totalSum / totalCount;
                double totalVar = (cardinality * (oldVar + (totalAvg - oldAvg) * (totalAvg - oldAvg))
                        + newCount * (newVar + (totalAvg - newAvg) * (totalAvg - newAvg))) / totalCount;
                double errorBound = tableMetaMap.get(column).getE();
                batchMetaMap.put(column, MetaInfo.calc(column, totalVar, totalCount, totalSum, errorBound, confidence));
                AdaLogger.debug(this, "Batch[" + context.getBatchCount() + "] table meta[" + column.getColumnName() + "]: " + batchMetaMap.get(column).toString());
                // REPORT: table.variance.{column}
                context.writeIntoReport("table.variance." + column.getColumnName(), totalVar);
                // REPORT: error-bound.{column}
                context.writeIntoReport("error-bound." + column.getColumnName(), errorBound);
            }
        }

        //group statistical
        String sampleName = "/home/scidb/zyz/tpch/10G/uniformsampleSQL";
        String scoreName = "/home/scidb/zyz/tpch/10G/uniformscore";
        Map<String, Double> sqlScoreMap = sqlScore(sampleName, scoreName);
        Iterator<String> iterator = sqlScoreMap.keySet().iterator();
        Map<Double, Double> groupSampleSizeMap = Maps.newHashMap();
        while (iterator.hasNext()) {
            String groupName = iterator.next();
            Double score = sqlScoreMap.get(groupName);
            if(score > 0) {
                String tableGroupSql = String.format("SELECT %s FROM %s.%s where %s", metaClause,
                        tableSchema.getDbName(), tableSchema.getTableName(), groupName);
                context.getDbmsSpark2().execute(tableGroupSql);
                long oldGroupCount = context.getDbmsSpark2().getResultAsLong(0, "count");
                double maxGroupSampleSize = 0;
                for (TableColumn column : tableSchema.getColumns()) {
                    if (inspectingColumns.contains(column) && (column.getColumnType().isInt() || column.getColumnType().isDouble())) {
                        double oldGroupVar = context.getDbmsSpark2().getResultAsDouble(0, "var_pop_" + column.getColumnName());
                        double oldGroupSum = column.getColumnType().isDouble() ?
                                context.getDbmsSpark2().getResultAsDouble(0, "sum_" + column.getColumnName()) :
                                context.getDbmsSpark2().getResultAsLong(0, "sum_" + column.getColumnName());
                        double oldGroupAvg = oldGroupSum / oldGroupCount;

                        String batchGroupSql = String.format("SELECT %s FROM %s.%s where %s", metaClause,
                                adaBatch.getDbName(), adaBatch.getTableName(), groupName);
                        context.getDbmsSpark2().execute(batchGroupSql);
                        long newGroupCount = context.getDbmsSpark2().getResultAsLong(0, "count");
                        long totalGroupCount = newGroupCount + oldGroupCount;

                        double newGroupVar = context.getDbmsSpark2().getResultAsDouble(0, "var_pop_" + column.getColumnName());
                        double newGroupSum = column.getColumnType().isDouble() ?
                                context.getDbmsSpark2().getResultAsDouble(0, "sum_" + column.getColumnName()) :
                                context.getDbmsSpark2().getResultAsLong(0, "sum_" + column.getColumnName());
                        double newGroupAvg = newGroupSum / newGroupCount;

                        double totalGroupSum = newGroupSum + oldGroupSum;
                        double totalGroupAvg = totalGroupSum / totalGroupCount;
                        double totalGroupVar = (cardinality * (oldGroupVar + (totalGroupAvg - oldGroupAvg) * (totalGroupAvg - oldGroupAvg))
                                + newCount * (newGroupVar + (totalGroupAvg - newGroupAvg) * (totalGroupAvg - newGroupAvg))) / totalGroupCount;

                        double errorBound = tableMetaMap.get(column).getE();

                        MetaInfo tempMetaInfo = MetaInfo.calc(column, totalGroupVar, totalGroupCount, totalGroupSum, errorBound, confidence);
                        tempMetaInfo.setN(tempMetaInfo.getN() * totalCount / totalGroupCount);
                        if(tempMetaInfo.getN() > maxGroupSampleSize) {
                            maxGroupSampleSize = tempMetaInfo.getN();
                        }
                    }
                }

                if(groupSampleSizeMap.containsKey(maxGroupSampleSize)) {
                    groupSampleSizeMap.put(maxGroupSampleSize, groupSampleSizeMap.get(maxGroupSampleSize) + score);
                } else {
                    groupSampleSizeMap.put(maxGroupSampleSize, score);
                }
            }
        }

        ArrayList<Map.Entry<Double, Double>> templist = new ArrayList<>();
        for(Map.Entry<Double, Double> entry : groupSampleSizeMap.entrySet()){
            templist.add(entry);
        }
        templist.sort((Comparator<Map.Entry<Double, Double>>) (o1, o2) -> {
            if(o2.getValue() > o1.getValue())
                return 1;
            else
                return -1;
        });
        double maxSampleSize = 0;
        for(int i = 0;i < 5;i++) {
            if(templist.get(i).getKey() > maxSampleSize) {
                maxSampleSize = templist.get(i).getKey();
            }
        }

        for (TableColumn column : tableSchema.getColumns()) {
            if (inspectingColumns.contains(column) && (column.getColumnType().isInt() || column.getColumnType().isDouble())) {
                if (batchMetaMap.get(column).getN() < maxSampleSize) {
                    batchMetaMap.get(column).setN(maxSampleSize);
                    batchMetaMap.get(column).setX((int) Math.floor(maxSampleSize));
                    break;
                }
            }
        }

        cardinality += newCount;
        tableMetaMap = batchMetaMap;

        AdaLogger.info(this, String.format("Table[%s] cardinality: %d", tableSchema.getTableName(), cardinality));
        context.set(tableSchema.getTableName() + "_cardinality", String.valueOf(cardinality));

        return this;
    }

//
//    public TableMeta refresh(AdaBatch adaBatch) {
//        double confidence = Double.parseDouble(context.get("query.confidence_internal_"));
//        String sql = String.format("SELECT %s FROM %s.%s", metaClause,
//                adaBatch.getDbName(), adaBatch.getTableName());
//        context.getDbmsSpark2().execute(sql);
//        int newCount = adaBatch.getSize();
//        long totalCount = newCount + cardinality;
//        Map<TableColumn, MetaInfo> batchMetaMap = Maps.newHashMap();
//        for (TableColumn column : tableSchema.getColumns()) {
//            if (inspectingColumns.contains(column) && (column.getColumnType().isInt() || column.getColumnType().isDouble())) {
//                double oldVar = tableMetaMap.get(column).getS2();
//                double oldSum = tableMetaMap.get(column).getSum();
//                double oldAvg = tableMetaMap.get(column).getAvg();
//                double newVar = context.getDbmsSpark2().getResultAsDouble(0, "var_pop_" + column.getColumnName());
//                double newSum = column.getColumnType().isDouble() ?
//                        context.getDbmsSpark2().getResultAsDouble(0, "sum_" + column.getColumnName()) :
//                        context.getDbmsSpark2().getResultAsLong(0, "sum_" + column.getColumnName());
//                double newAvg = newSum / newCount;
//                double totalSum = newSum + oldSum;
//                double totalAvg = totalSum / totalCount;
//                double totalVar = (cardinality * (oldVar + (totalAvg - oldAvg) * (totalAvg - oldAvg))
//                        + newCount * (newVar + (totalAvg - newAvg) * (totalAvg - newAvg))) / totalCount;
//                double errorBound = tableMetaMap.get(column).getE();
//                batchMetaMap.put(column, MetaInfo.calc(column, totalVar, totalCount, totalSum, errorBound, confidence));
//                AdaLogger.debug(this, "Batch[" + context.getBatchCount() + "] table meta[" + column.getColumnName() + "]: " + batchMetaMap.get(column).toString());
//                // REPORT: table.variance.{column}
//                context.writeIntoReport("table.variance." + column.getColumnName(), totalVar);
//                // REPORT: error-bound.{column}
//                context.writeIntoReport("error-bound." + column.getColumnName(), errorBound);
//            }
//        }
//        cardinality += newCount;
//        tableMetaMap = batchMetaMap;
//
//        AdaLogger.info(this, String.format("Table[%s] cardinality: %d", tableSchema.getTableName(), cardinality));
//        context.set(tableSchema.getTableName() + "_cardinality", String.valueOf(cardinality));
//
//        return this;
//    }

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


    private Map<String, Double> sqlScore(String sampleName, String scoreName){
        Map<String, Double> sqlScoreMap = Maps.newHashMap();
        try {
            FileReader sqlFile = new FileReader(sampleName);
            FileReader scoreFile = new FileReader(scoreName);
            BufferedReader sqlReader = new BufferedReader(sqlFile);
            BufferedReader scoreReader = new BufferedReader(scoreFile);
            String str;
            ArrayList<String> sqlList = new ArrayList<String>();
            while((str=sqlReader.readLine())!= null) {
                sqlList.add(str.replace('|', ' '));
            }
            ArrayList<Double> scoreList = new ArrayList<Double>();
            while((str=scoreReader.readLine())!= null) {
                scoreList.add(Double.parseDouble(str));
            }
            for(int i = 0;i < sqlList.size();i++) {
                sqlScoreMap.put(sqlList.get(i), scoreList.get(i));
            }
            sqlFile.close();
            scoreFile.close();
            sqlReader.close();
            scoreReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sqlScoreMap;
    }
}
