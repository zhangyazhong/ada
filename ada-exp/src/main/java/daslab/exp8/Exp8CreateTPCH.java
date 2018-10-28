package daslab.exp8;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import daslab.exp.ExpTemplate;
import daslab.inspector.TableColumn;
import daslab.inspector.TableColumnType;
import daslab.utils.AdaLogger;
import daslab.utils.AdaSystem;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.List;
import java.util.stream.Collectors;

import static daslab.inspector.TableColumnType.INT;
import static daslab.inspector.TableColumnType.DATE;
import static daslab.inspector.TableColumnType.DOUBLE;
import static daslab.inspector.TableColumnType.STRING;

@SuppressWarnings("Duplicates")
public class Exp8CreateTPCH extends ExpTemplate {
    class TPCHTable {
        String tableName;
        List<TableColumn> tableStructure;
        String tableTerminated;

        TPCHTable(String tableName) {
            this.tableName = tableName;
            this.tableTerminated = "|";
            this.tableStructure = Lists.newLinkedList();
        }

        private TPCHTable addColumn(String name, TableColumnType type) {
            this.tableStructure.add(new TableColumn(tableStructure.size(), name, type));
            return this;
        }

        private String getTableName() {
            return tableName;
        }

        private String getTableTerminated() {
            return tableTerminated;
        }

        private String getTableStructureInSQL() {
            return StringUtils.join(tableStructure.stream().map(column -> column.getColumnName() + " " + column.getColumnType().toString()).toArray(), ", ");
        }
    }

    public Exp8CreateTPCH() {
        this("Ada Exp8 - Create Database for TPC-H");
    }

    public Exp8CreateTPCH(String name) {
        super(name);
    }

    @Override
    public void run() {
        AdaSystem.call("hadoop fs -rm -r " + get("data.table.hdfs.location"));
        AdaSystem.call("hadoop fs -rm -r " + get("batch.table.hdfs.location"));
        execute(String.format("DROP DATABASE IF EXISTS %s CASCADE", get("data.table.schema")));
        execute(String.format("CREATE DATABASE %s", get("data.table.schema")));
        execute(String.format("USE %s", get("data.table.schema")));
        execute(String.format("CREATE EXTERNAL TABLE %s(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s' LOCATION '%s/'", get("data.table.name"), get("data.table.structure"), get("data.table.terminated"), get("data.table.hdfs.location")));
        execute(String.format("CREATE EXTERNAL TABLE %s(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s' LOCATION '%s/'", get("batch.table.name"), get("batch.table.structure"), get("batch.table.terminated"), get("batch.table.hdfs.location")));
        String path = get("base.hdfs.location.pattern");
//        String command = "hadoop fs -D dfs.replication=1 -mv " + path + " " + get("data.table.hdfs.location");
        String command = "hadoop fs -mv " + path + " " + get("data.table.hdfs.location");
        AdaSystem.call(command);

        TPCHTable supplier = new TPCHTable("supplier")
                .addColumn("s_suppkey", INT)
                .addColumn("s_name", STRING)
                .addColumn("s_address", STRING)
                .addColumn("s_nationkey", INT)
                .addColumn("s_phone", STRING)
                .addColumn("s_acctbal", INT)
                .addColumn("s_comment", STRING);

        TPCHTable region = new TPCHTable("region")
                .addColumn("r_regionkey", INT)
                .addColumn("r_name", STRING)
                .addColumn("r_comment", STRING);

        TPCHTable customer = new TPCHTable("customer")
                .addColumn("c_custkey", INT)
                .addColumn("c_name", STRING)
                .addColumn("c_address", STRING)
                .addColumn("c_nationkey", INT)
                .addColumn("c_phone", STRING)
                .addColumn("c_acctbal", DOUBLE)
                .addColumn("c_mktsegment", STRING)
                .addColumn("c_comment", STRING);

        TPCHTable part = new TPCHTable("part")
                .addColumn("p_partkey", INT)
                .addColumn("p_name", STRING)
                .addColumn("p_mfgr", STRING)
                .addColumn("p_brand", STRING)
                .addColumn("p_type", STRING)
                .addColumn("p_size", INT)
                .addColumn("p_container", STRING)
                .addColumn("p_retailprice", DOUBLE)
                .addColumn("p_comment", STRING);

        TPCHTable partsupp = new TPCHTable("partsupp")
                .addColumn("ps_partkey", INT)
                .addColumn("ps_suppkey", INT)
                .addColumn("ps_availqty", INT)
                .addColumn("ps_supplycost", DOUBLE)
                .addColumn("ps_comment", STRING);

        TPCHTable orders = new TPCHTable("orders")
                .addColumn("o_orderkey", INT)
                .addColumn("o_custkey", INT)
                .addColumn("o_orderstatus", STRING)
                .addColumn("o_totalprice", DOUBLE)
                .addColumn("o_orderdate", DATE)
                .addColumn("o_orderpriority", STRING)
                .addColumn("o_clerk", STRING)
                .addColumn("o_shippriority", INT)
                .addColumn("o_comment", STRING);

        TPCHTable nation = new TPCHTable("nation")
                .addColumn("n_nationkey", INT)
                .addColumn("n_name", STRING)
                .addColumn("n_regionkey", INT)
                .addColumn("n_comment", STRING);

        ImmutableList.of(supplier, region, customer, part, partsupp, orders, nation).forEach(table -> {
            execute(String.format("CREATE EXTERNAL TABLE %s(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s' LOCATION '%s/'", table.getTableName(), table.getTableStructureInSQL(), table.getTableTerminated(), "/zyz/spark/variance_" + table.getTableName()));
//            AdaSystem.call(String.format("hadoop fs -D dfs.replication=1 -mv %s %s", "/zyz/stratified/" + table.getTableName(), "/zyz/spark/stratified_" + table.getTableName()));
            AdaSystem.call(String.format("hadoop fs -mv %s %s", "/zyz/variance/" + table.getTableName(), "/zyz/spark/variance_" + table.getTableName()));
            AdaLogger.info(this, "Table " + table.getTableName() + " finished.");
        });

        ImmutableList.of(supplier, region, customer, part, partsupp, orders, nation).forEach(table -> {
            String sql = String.format("SELECT COUNT(*) FROM %s", table.getTableName());
            JSONArray jsonArray = new JSONArray(getSpark().sql(sql).toJSON().collectAsList().stream().map(jsonString -> {
                JSONObject jsonObject = new JSONObject();
                try {
                    jsonObject = new JSONObject(jsonString);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                return jsonObject;
            }).collect(Collectors.toList()));
            AdaLogger.info(this, String.format("{%s}: %s", sql, jsonArray.toString()));
        });
    }
}
