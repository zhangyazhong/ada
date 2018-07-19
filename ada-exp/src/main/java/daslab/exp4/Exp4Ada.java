package daslab.exp4;

import com.google.common.collect.ImmutableList;
import daslab.context.AdaContext;
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

@SuppressWarnings("Duplicates")
public class Exp4Ada extends ExpTemplate {
    private final static int REPEAT_TIME = 10;

    private final static List<String> QUERIES = ImmutableList.of(
            String.format("SELECT AVG(page_count) FROM %s.%s", ExpConfig.get("table.schema"), ExpConfig.get("table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE page_size>80000",  ExpConfig.get("table.schema"), ExpConfig.get("table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='aa'", ExpConfig.get("table.schema"), ExpConfig.get("table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='kk'",  ExpConfig.get("table.schema"), ExpConfig.get("table.name"))
    );

    public Exp4Ada() {
        this("Ada Exp4 - Ada Result for Queries");
    }

    public Exp4Ada(String name) {
        super(name);
    }

    @Override
    public void run() {
        ExpResult expResult = new ExpResult(Exp4Verdict.generateHeader());
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            AdaContext context = new AdaContext();
            context.start();
            for (int i = ExpConfig.HOUR_START; i < ExpConfig.HOUR_TOTAL; i++) {
                int day = i / 24 + 1;
                int hour = i % 24;
                String time = String.format("%02d%02d", day, hour);
                String location = String.format("/home/hadoop/wiki/n_pagecounts-201601%02d-%02d0000", day, hour);
                AdaLogger.info(this, "Send a new batch at " + location);
                context.receive(location);
                try {
                    for (String QUERY : QUERIES) {
                        Row row = getVerdict().sql(QUERY).first();
                        double avg = row.getDouble(0);
                        double err = row.getDouble(1);
                        expResult.addResult(time, String.format("%.8f/%.8f", avg, err));
                    }
                } catch (VerdictException e) {
                    e.printStackTrace();
                }
                AdaLogger.info(this, String.format("Ada Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ", ")));
            }
        }
        save(expResult, "/tmp/ada/exp/exp4/exp4_ada");
    }
}
