package daslab.restore;

import daslab.exp.ExpConfig;
import daslab.exp.ExpTemplate;
import daslab.utils.AdaLogger;
import daslab.utils.AdaSystem;
import edu.umich.verdict.exceptions.VerdictException;

public class DatabaseRestore extends ExpTemplate implements RestoreModule {
    public DatabaseRestore() {
        this("Ada Exp - DatabaseRestore");
    }

    public DatabaseRestore(String name) {
        super(name);
    }

    public void restore() {
        AdaSystem.call("hadoop fs -rm -r " + get("data.table.hdfs.location"));
        execute(String.format("DROP DATABASE IF EXISTS %s CASCADE", get("data.table.schema")));
        execute(String.format("CREATE DATABASE %s", get("data.table.schema")));
        execute(String.format("USE %s", get("data.table.schema")));
        execute(String.format("CREATE EXTERNAL TABLE %s(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '%s/'", get("data.table.name"), get("data.table.hdfs.location")));
        execute(String.format("CREATE TABLE %s(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','", get("batch.table.name")));
        for (int i = 0; i < ExpConfig.HOUR_START; i++) {
            int day = i / 24 + 1;
            int hour = i % 24;
            String path = String.format(get("source.hdfs.location.pattern"), day, hour);
            String command = "hadoop fs -cp " + path + " " + get("data.table.hdfs.location");
            AdaLogger.debug(this, "Loading " + path + " into table");
            AdaSystem.call(command);
        }

        AdaLogger.info(this, "Restored database to initial status.");

        try {
            execute(String.format("DROP DATABASE IF EXISTS %s CASCADE", get("sample.table.schema")));
            execute(String.format("CREATE DATABASE %s", get("sample.table.schema")));
            for (int ratio : ExpConfig.UNIFORM_SAMPLE_RATIO) {
                getVerdict().sql(String.format("CREATE %d%% UNIFORM SAMPLE OF %s.%s", ratio, get("data.table.schema"), get("data.table.name")));
            }
            for (int i = 0; i < ExpConfig.STRATIFIED_SAMPLE_RATIO.length; i++) {
                getVerdict().sql(String.format("CREATE %d%% STRATIFIED SAMPLE OF %s.%s ON %s", ExpConfig.STRATIFIED_SAMPLE_RATIO[i], get("data.table.schema"), get("data.table.name"), ExpConfig.STRATIFIED_SAMPLE_COLUMN[i]));
            }
        } catch (VerdictException e) {
            e.printStackTrace();
        }

        AdaLogger.info(this, "Restored sample to initial status.");
    }

    @Override
    public void run() {
        restore();
    }

    public static void main(String[] args) {
        DatabaseRestore d = new DatabaseRestore();
        d.restore();
    }
}
