package daslab.exp7;

import com.google.common.collect.ImmutableList;
import daslab.context.AdaContext;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

import static daslab.exp.ExpConfig.*;

/**
 * @author zyz
 * @version 2018-10-04
 */
public class Exp7AdaResult extends ExpTemplate {
    private final static int REPEAT_TIME = 10;
    public final static String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp7/verdict_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    private static List<String> QUERIES;

    public Exp7AdaResult() {
        this("Ada Exp7 - Ada Result Accuracy Test (TPC-H)");
    }

    public Exp7AdaResult(String name) {
        super(name);
    }

    @Override
    public void run() {
        QUERIES = ImmutableList.of(
                "SELECT sum(l_extendedprice * l_discount) as revenue FROM tpch.lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01 AND l_quantity < 24",
                "SELECT 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue FROM tpch.lineitem, tpch.part WHERE l_partkey = p_partkey AND l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'",
                "SELECT sum(l_extendedprice* (1 - l_discount)) as revenue FROM tpch.lineitem, tpch.part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size between 1 AND 5 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size between 1 AND 10 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR ( p_partkey = l_partkey AND p_brand = 'Brand#34' AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size between 1 AND 15 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"
        );
        ExpResult expResult = new ExpResult("time");
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            resetVerdict();
            AdaContext context = new AdaContext().enableForceResample(true).start();
            for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
                String[] locations = new String[HOUR_INTERVAL];
                String time = String.format("%02d~%02d", i, (i + HOUR_INTERVAL - 1));
                for (int j = i; j < i + HOUR_INTERVAL; j++) {
                    locations[j - i] = String.format(get("source.hdfs.location.pattern"), j);
                }
                i = i + HOUR_INTERVAL - 1;
                AdaLogger.info(this, "Send a new batch at " + Arrays.toString(locations));
                context.receive(locations);
                try {
                    runQueryByVerdict(expResult, QUERIES, time, k);
                } catch (VerdictException e) {
                    e.printStackTrace();
                }
                AdaLogger.info(this, String.format("Ada Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ExpResult.SEPARATOR)));
                expResult.save(RESULT_SAVE_PATH);
            }
            expResult.save(RESULT_SAVE_PATH);
        }
    }
}
