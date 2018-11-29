package daslab.exp21;

import daslab.bean.ExecutionReport;
import daslab.bean.Sample;
import daslab.bean.Sampling;
import daslab.context.AdaContext;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;

import java.util.Arrays;
import java.util.Map;

import static daslab.exp.ExpConfig.HOUR_INTERVAL;
import static daslab.exp.ExpConfig.HOUR_START;
import static daslab.exp.ExpConfig.HOUR_TOTAL;

/**
 * @author sissors-lab
 * @version 2018-11-28
 */
public class Exp21TimeCost extends ExpTemplate {
    public final static String ADA_RESULT_PATH = String.format("/tmp/ada/exp/exp21/ada_cost_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);
    public final static String RESAMPLE_RESULT_PATH = String.format("/tmp/ada/exp/exp21/resample_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    public Exp21TimeCost() {
        this("Ada Exp21 - Time Cost Test");
    }

    public Exp21TimeCost(String name) {
        super(name);
    }

    @Override
    public void run() {
        String expStrategy = get("exp.strategy");
        ExpResult expResult = new ExpResult("time");
        SystemRestore.restoreModules().forEach(RestoreModule::restore);
        AdaLogger.info(this, "Restored database.");
        resetVerdict();
        AdaContext context;
        if (expStrategy.equals("resample")) {
            context = new AdaContext().enableForceResample(true).start();
        } else {
            context = new AdaContext().start();
        }
        for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
            String[] locations = new String[HOUR_INTERVAL];
            String time = String.format("%02d%02d~%02d%02d",
                    i / 24 + 1, i % 24, (i + HOUR_INTERVAL - 1) / 24 + 1, (i + HOUR_INTERVAL - 1) % 24);
            for (int j = 0; j < HOUR_INTERVAL; j++) {
                int day = (i + j) / 24 + 1;
                int hour = (i + j) % 24;
                locations[j] = String.format(get("source.hdfs.location.pattern"), day, hour);
            }
            i = i + HOUR_INTERVAL - 1;
            AdaLogger.info(this, "Send a new batch at " + Arrays.toString(locations));
            ExecutionReport executionReport = context.receive(locations);
            for (Map.Entry<String, Object> entry : executionReport.search("sampling.cost").entrySet()) {
                expResult.push(time, entry.getKey(), String.valueOf(entry.getValue()));
            }
            for (Map.Entry<String, Object> entry : executionReport.search("sample").entrySet()) {
                expResult.push(time, entry.getKey(), String.valueOf(entry.getValue()));
            }
            expResult.push(time, "strategy", ((Map<Sample, Sampling>) executionReport.get("sampling.strategies"))
                    .entrySet()
                    .stream()
                    .map(e -> e.getKey().sampleType + ":" + e.getValue().toString() + ";")
                    .reduce((s1, s2) -> s1 + s2)
                    .orElse(""));
            if (expStrategy.equals("resample")) {
                expResult.save(RESAMPLE_RESULT_PATH);
            } else {
                expResult.save(ADA_RESULT_PATH);
            }
        }
        if (expStrategy.equals("resample")) {
            expResult.save(RESAMPLE_RESULT_PATH);
        } else {
            expResult.save(ADA_RESULT_PATH);
        }
    }
}
