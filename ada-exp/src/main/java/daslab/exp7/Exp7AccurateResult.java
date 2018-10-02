package daslab.exp7;

import com.google.common.collect.ImmutableList;
import daslab.context.AdaContext;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

import static daslab.exp.ExpConfig.HOUR_INTERVAL;
import static daslab.exp.ExpConfig.HOUR_START;
import static daslab.exp.ExpConfig.HOUR_TOTAL;

public class Exp7AccurateResult extends ExpTemplate {
    public final static String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp7/accurate_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    public Exp7AccurateResult() {
        this("Ada Exp7 - Accuracy Result (TPC-H)");
    }

    public Exp7AccurateResult(String name) {
        super(name);
    }

    @Override
    public void run() {
        List<String> QUERIES;
        // Q6, Q14, Q19
        QUERIES = ImmutableList.of(
                "SELECT sum(l_extendedprice * l_discount) as revenue FROM tpch.lineitem WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1994-01-01' + interval '1' year AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01 AND l_quantity < 24",
                "SELECT 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue FROM tpch.lineitem, tpch.part WHERE l_partkey = p_partkey AND l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'",
                "SELECT sum(l_extendedprice* (1 - l_discount)) as revenue FROM lineitem, part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size between 1 AND 5 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size between 1 AND 10 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR ( p_partkey = l_partkey AND p_brand = 'Brand#34' AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size between 1 AND 15 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"
        );
        ExpResult expResult = new ExpResult("time");
        SystemRestore.restoreModules().forEach(RestoreModule::restore);
        AdaLogger.info(this, "Restored database.");
        resetVerdict();
        AdaContext context = new AdaContext().skipSampling(true).start();
        for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
            String[] locations = new String[HOUR_INTERVAL];
            String time = String.format("%02d~%02d", i, (i + HOUR_INTERVAL - 1));
            for (int j = i; j < i + HOUR_INTERVAL; j++) {
                locations[j - i] = String.format(get("source.hdfs.location.pattern"), j);
            }
            i = i + HOUR_INTERVAL - 1;
            AdaLogger.info(this, "Send a new batch at " + Arrays.toString(locations));
            context.receive(locations);
            runQueryBySpark(expResult, QUERIES, time);
            AdaLogger.info(this, String.format("Accurate Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ExpResult.SEPARATOR)));
            expResult.save(RESULT_SAVE_PATH);
        }
        expResult.save(RESULT_SAVE_PATH);
    }
}
