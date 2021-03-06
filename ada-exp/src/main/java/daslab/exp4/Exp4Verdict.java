package daslab.exp4;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import daslab.exp.ExpConfig;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Row;

import java.util.List;

public class Exp4Verdict extends ExpTemplate {
    private final static int SAMPLE_COUNT = 10;

    private final static List<String> QUERIES = ImmutableList.of(
            String.format("SELECT AVG(page_count) FROM %s.%s", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE page_size>80000",  ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='aa'", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='kk'",  ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name"))
    );

    public Exp4Verdict() {
        this("Ada Exp4 - Verdict Result for Queries");
    }

    public Exp4Verdict(String name) {
        super(name);
    }

    @Override
    public void run() {
        SystemRestore.restoreModules().forEach(RestoreModule::restore);
        AdaLogger.info(this, "Restored database.");
        ExpResult expResult = new ExpResult(generateHeader());
        for (int i = ExpConfig.HOUR_START; i < ExpConfig.HOUR_TOTAL; i++) {
            int day = i / 24 + 1;
            int hour = i % 24;
            String time = String.format("%02d%02d", day, hour);
            append(day, hour);
            for (int j = 0; j < SAMPLE_COUNT; j++) {
                try {
                    String sampling = String.format("CREATE 10%% UNIFORM SAMPLE OF %s.%s", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name"));
                    getVerdict().execute(sampling);
                    for (String QUERY : QUERIES) {
                        Row row = getVerdict().sql(QUERY).first();
                        double avg = row.getDouble(0);
                        double err = row.getDouble(1);
                        expResult.addResult(time, String.format("%.8f/%.8f", avg, err));
                    }
                    sampling = String.format("DROP SAMPLES OF %s.%s", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name"));
                    getVerdict().execute(sampling);
                } catch (VerdictException e) {
                    e.printStackTrace();
                }
            }
            AdaLogger.info(this, String.format("Verdict Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ", ")));
        }
        save(expResult, "/tmp/ada/exp/exp4/exp4_verdict");
    }

    /**
     * "q0_0", "q1_0", "q2_0", "q3_0", "q0_1", "q1_1"...
     *  first number for query No. and second number for sample No.
     *
     * @return header list
     */
    public static List<String> generateHeader() {
        List<String> header = Lists.newLinkedList();
        header.add("time");
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            for (int j = 0; j < QUERIES.size(); j++) {
                header.add("q" + j + "_" + i);
            }
        }
        return header;
    }
}
