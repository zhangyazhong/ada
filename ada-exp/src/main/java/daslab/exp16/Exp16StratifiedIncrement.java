package daslab.exp16;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import daslab.bean.ExecutionReport;
import daslab.bean.Sample;
import daslab.bean.Sampling;
import daslab.context.AdaContext;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;

import java.util.List;
import java.util.Map;

/**
 * @author zyz
 * @version 2018-10-15
 */
public class Exp16StratifiedIncrement extends ExpTemplate {
    private final static int REPEAT_TIME = 10;
    public final String COST_SAVE_PATH = String.format("/tmp/ada/exp/exp16/stratified_cost_%s.csv", get("exp.stratified.group"));
    public final String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp16/stratified_result_%s.csv", get("exp.stratified.group"));

    private Map<String, String> distribution = ImmutableMap.<String, String>builder()
            .put("20", "1994-01-01,1994-01-10,1995-09-01,1995-09-10")
            .put("40", "1994-01-01,1994-01-20,1995-09-01,1995-09-20")
            .put("60", "1994-01-01,1994-01-30,1995-09-01,1995-09-30")
            .put("80", "1994-01-01,1994-02-20,1995-09-01,1995-09-30")
            .put("100", "1994-01-01,1994-03-10,1995-09-01,1995-09-30")
            .put("120", "1994-01-01,1994-03-30,1995-09-01,1995-09-30")
            .put("140", "1994-01-01,1994-04-20,1995-09-01,1995-09-30")
            .put("160", "1994-01-01,1994-05-10,1995-09-01,1995-09-30")
            .put("180", "1994-01-01,1994-05-30,1995-09-01,1995-09-30")
            .put("200", "1994-01-01,1994-06-10,1995-09-01,1995-09-30")
            .build();

    public Exp16StratifiedIncrement() {
        this("Ada Exp16 - Stratified Count Increment");
    }

    public Exp16StratifiedIncrement(String name) {
        super(name);
    }

    @Override
    public void run() {
        List<String> QUERIES;
        QUERIES = ImmutableList.of(
                "SELECT sum(l_extendedprice * l_discount) as revenue FROM tpch.lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01 AND l_quantity < 24",
                "SELECT 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue FROM tpch.lineitem, tpch.part WHERE l_partkey = p_partkey AND l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'"
        );

        ExpResult expCostResult = new ExpResult("time");
        ExpResult expResult = new ExpResult("time");
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            resetVerdict();
            AdaContext context = new AdaContext()
                    .enableForceResample(true)
                    .start();
            for (int i = 0; i < 1; i++) {
                String time = String.format("%02d~%02d", i, i);
                execute(String.format("DROP TABLE IF EXISTS %s.%s", get("data.table.schema"), "lineitem_batch_tmp"));
                execute(String.format("CREATE TABLE %s.%s AS (SELECT * FROM tpch.lineitem WHERE (l_shipdate>='%s' AND l_shipdate<='%s') OR (l_shipdate>='%s' AND l_shipdate<='%s') LIMIT 800000)",
                        get("data.table.schema"), "lineitem_batch_tmp",
                        distribution.get(get("exp.stratified.group")).split(",")[0],
                        distribution.get(get("exp.stratified.group")).split(",")[1],
                        distribution.get(get("exp.stratified.group")).split(",")[2],
                        distribution.get(get("exp.stratified.group")).split(",")[3]
                ));
                execute(String.format("INSERT INTO %s.%s AS (SELECT * FROM tpch.lineitem WHERE (l_shipdate>='%s' AND l_shipdate<='%s') OR (l_shipdate>='%s' AND l_shipdate<'%s') LIMIT 7200000)",
                        get("data.table.schema"), "lineitem_batch_tmp",
                        "1996-01-01", "1998-12-01",
                        "1992-01-01", "1994-01-01"
                ));
                AdaLogger.info(this, "Send a new batch at " + get("data.table.schema") + ".lineitem_batch_tmp");
                ExecutionReport executionReport = context.receive(get("data.table.schema"), "lineitem_batch_tmp");
                for (Map.Entry<String, Object> entry : executionReport.search("sampling.cost").entrySet()) {
                    expCostResult.push(time, entry.getKey(), String.valueOf(entry.getValue()));
                }
                expCostResult.push(time, "strategy", ((Map<Sample, Sampling>) executionReport.get("sampling.strategies"))
                        .entrySet()
                        .stream()
                        .map(e -> e.getKey().sampleType + ":" + e.getValue().toString() + ";")
                        .reduce((s1, s2) -> s1 + s2)
                        .orElse(""));
                expCostResult.save(COST_SAVE_PATH);
                try {
                    runQueryByVerdict(expResult, QUERIES, time, k);
                } catch (VerdictException e) {
                    e.printStackTrace();
                }
                expResult.save(RESULT_SAVE_PATH);
            }
            expCostResult.save(COST_SAVE_PATH);
            expResult.save(RESULT_SAVE_PATH);
        }
    }
}
