package daslab.exp5;

import daslab.exp.ExpConfig;
import daslab.exp.ExpTemplate;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

@SuppressWarnings("Duplicates")
public class Exp5Restore extends ExpTemplate {
    public Exp5Restore() {
        this("Ada Exp5 - Exp5 Restore");
    }

    public Exp5Restore(String name) {
        super(name);
    }

    @Override
    public void run() {
        call("hadoop fs -rm -r /home/hadoop/spark/wiki_ada_pagecounts");
        execute("DROP DATABASE IF EXISTS wiki_ada CASCADE");
        execute("CREATE DATABASE wiki_ada");
        execute("USE wiki_ada");
        execute("CREATE EXTERNAL TABLE pagecounts(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/home/hadoop/spark/wiki_ada_pagecounts/'");
        execute("CREATE TABLE pagecounts_batch(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        for (int i = 0; i < ExpConfig.HOUR_START; i++) {
            int day = i / 24 + 1;
            int hour = i % 24;
            String path = String.format("/home/hadoop/wiki/n_pagecounts-201601%02d-%02d0000", day, hour);
            String command = "hadoop fs -cp " + path + " /home/hadoop/spark/wiki_ada_pagecounts/";
            AdaLogger.debug(this, "Loading " + path + " into table");
            call(command);
        }

        AdaLogger.info(this, "Restored database to initial status.");

        try {
            getSpark().sql("DROP DATABASE IF EXISTS wiki_ada_verdict CASCADE");
            getVerdict().sql("DROP SAMPLES OF wiki_ada.pagecounts");
            for (int ratio : ExpConfig.UNIFORM_SAMPLE_RATIO) {
                getVerdict().sql("CREATE " + ratio + "% UNIFORM SAMPLE OF wiki_ada.pagecounts");
            }
            for (int i = 0; i < ExpConfig.STRATIFIED_SAMPLE_RATIO.length; i++) {
                getVerdict().sql("CREATE " + ExpConfig.STRATIFIED_SAMPLE_RATIO[i] + "% STRATIFIED SAMPLE OF wiki_ada.pagecounts ON " + ExpConfig.STRATIFIED_SAMPLE_COLUMN[i]);
            }
        } catch (VerdictException e) {
            e.printStackTrace();
        }

        AdaLogger.info(this, "Restored sample to initial status.");
    }

    private void call(String cmd) {
        AdaLogger.info(this, "About to call: " + cmd);
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(process.getInputStream())));
            String line;
            while ((line = bufferedReader.readLine()) != null)
                AdaLogger.debug(this, "System print: " + line);
            if (process.waitFor() != 0) {
                if (process.exitValue() == 1) {
                    AdaLogger.error(this, "Call {" + cmd + "} error!");
                }
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
