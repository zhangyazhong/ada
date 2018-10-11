package daslab.exp7;

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

import static daslab.exp.ExpConfig.*;

@SuppressWarnings("Duplicates")
public class Exp7AdaTimeCost extends ExpTemplate {
    private final static int REPEAT_TIME = 1;
    private final static String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp7/un_verdict_cost_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    public Exp7AdaTimeCost() {
        this("Ada Exp7 - Ada Time Cost (TPC-H)");
    }

    public Exp7AdaTimeCost(String name) {
        super(name);
    }

    @Override
    public void run() {
        ExpResult expResult = new ExpResult();
        expResult.addHeader("time");
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            resetVerdict();
            AdaContext context = new AdaContext().enableForceResample(true).enableAdaptive(false).start();
            for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
                String[] locations = new String[HOUR_INTERVAL];
                String time = String.format("%02d~%02d", i, (i + HOUR_INTERVAL - 1));
                for (int j = i; j < i + HOUR_INTERVAL; j++) {
                    locations[j - i] = String.format(get("source.hdfs.location.pattern"), j);
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
                expResult.save(RESULT_SAVE_PATH);
            }
        }
        expResult.save(RESULT_SAVE_PATH);
    }
}
