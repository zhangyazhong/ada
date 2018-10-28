package daslab.exp19;

import com.google.common.collect.ImmutableList;
import daslab.context.AdaContext;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;

import java.util.List;

public class Exp19VarianceIncrement extends ExpTemplate {
    private final static int REPEAT_TIME = 10;
    public final String RESAMPLE_RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp19/resample_result_%s.csv", get("exp.variance.increment"));
    public final String UPDATE_RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp19/update_result_%s.csv", get("exp.variance.increment"));
    public final String SPARK_RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp19/accurate_result_%s.csv", get("exp.variance.increment"));


    public Exp19VarianceIncrement() {
        this("Ada Exp19 - Variance Increment");
    }

    public Exp19VarianceIncrement(String name) {
        super(name);
    }

    @Override
    public void run() {
        List<String> QUERIES;
        QUERIES = ImmutableList.of(
                "SELECT sum(l_extendedprice * l_discount) as revenue FROM tpch_variance.lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01 AND l_quantity < 24",
                "SELECT sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) * 100.0 / sum(l_extendedprice * (1 - l_discount)) as promo_revenue FROM tpch_variance.lineitem, tpch_variance.part WHERE l_partkey = p_partkey AND l_shipdate >= '1994-09-01' AND l_shipdate < '1994-10-01'",
                "SELECT sum(l_extendedprice* (1 - l_discount)) as revenue FROM tpch_variance.lineitem, tpch_variance.part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size between 1 AND 5 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size between 1 AND 10 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR ( p_partkey = l_partkey AND p_brand = 'Brand#34' AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size between 1 AND 15 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"
        );
        ExpResult expResampleResult = new ExpResult("time");
        ExpResult expUpdateResult = new ExpResult("time");
        ExpResult expSparkResult = new ExpResult("time");
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            resetVerdict();
            AdaContext context = new AdaContext()
                    .enableForceResample(true)
//                    .enableForceUpdate(true)
                    .start();
            for (int i = 0; i < 1; i++) {
                String time = String.format("%02d~%02d", i, i);
                String location = String.format(get("source.hdfs.location.pattern"), Integer.valueOf(get("exp.variance.increment").trim()));
                context.receive(location);
                AdaLogger.info(this, "Send a new batch at " + location);
                try {
//                    runQueryByVerdict(expUpdateResult, QUERIES, time, k);
                    runQueryByVerdict(expResampleResult, QUERIES, time, k);
//                    if (k < 1) {
//                        runQueryBySpark(expSparkResult, QUERIES, time);
//                        expSparkResult.save(SPARK_RESULT_SAVE_PATH);
//                    }
                } catch (VerdictException e) {
                    e.printStackTrace();
                }
//                expUpdateResult.save(UPDATE_RESULT_SAVE_PATH);
                expResampleResult.save(RESAMPLE_RESULT_SAVE_PATH);
            }
//            expUpdateResult.save(UPDATE_RESULT_SAVE_PATH);
            expResampleResult.save(RESAMPLE_RESULT_SAVE_PATH);
        }
    }
}
