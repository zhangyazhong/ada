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
        if (get("profile").contains("tpch")) {
            AdaSystem.call("hadoop fs -rm -r " + get("data.table.hdfs.location") + "/lineitem_batch*");
            AdaSystem.call("hadoop fs -rm -r " + get("batch.table.hdfs.location") + "/*");
            /*
            for (int i = Integer.parseInt(get("exp.hour.init")); i < ExpConfig.HOUR_START; i++) {
                String path = String.format(get("source.hdfs.location.pattern"), i);
                String command = "hadoop fs -cp " + path + " " + get("data.table.hdfs.location");
                AdaLogger.debug(this, "Loading " + path + " into table");
                AdaSystem.call(command);
            }
            */
        } else {
            AdaSystem.call("hadoop fs -rm -r " + get("data.table.hdfs.location"));
            AdaSystem.call("hadoop fs -rm -r " + get("batch.table.hdfs.location"));
            execute(String.format("DROP DATABASE IF EXISTS %s CASCADE", get("data.table.schema")));
            execute(String.format("CREATE DATABASE %s", get("data.table.schema")));
            execute(String.format("USE %s", get("data.table.schema")));
            execute(String.format("CREATE EXTERNAL TABLE %s(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s' LOCATION '%s/'", get("data.table.name"), get("data.table.structure"), get("data.table.terminated"), get("data.table.hdfs.location")));
            execute(String.format("CREATE EXTERNAL TABLE %s(%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s' LOCATION '%s/'", get("batch.table.name"), get("batch.table.structure"), get("batch.table.terminated"), get("batch.table.hdfs.location")));
            for (int i = 0; i < ExpConfig.HOUR_START; i++) {
                int day = i / 24 + 1;
                int hour = i % 24;
                String path = String.format(get("source.hdfs.location.pattern"), day, hour);
                String command = "hadoop fs -cp " + path + " " + get("data.table.hdfs.location");
                AdaLogger.debug(this, "Loading " + path + " into table");
                AdaSystem.call(command);
            }
        }

        AdaLogger.info(this, "Restored database to initial status.");

        try {
            execute(String.format("DROP DATABASE IF EXISTS %s CASCADE", get("sample.table.schema")));
            execute(String.format("CREATE DATABASE %s", get("sample.table.schema")));
            getVerdict().sql("USE " + get("data.table.schema"));
            double sampleRatio = Double.parseDouble(get("sample.init.ratio"));
            String[] sampleTypes = get("sample.init.type").split(",");
            String[] columns = get("sample.init.stratified.column").split(",");
            String sql;
            for (String sampleType : sampleTypes) {
                switch (sampleType.toLowerCase().trim()) {
                    case "uniform":
                        sql = String.format("CREATE %.2f%% UNIFORM SAMPLE OF %s.%s", sampleRatio, get("data.table.schema"), get("data.table.name"));
                        AdaLogger.debug(this, "Uniform sample: " + sql);
                        getVerdict().sql(sql);
                        break;
                    case "stratified":
                        for (String column : columns) {
                            sql = String.format("CREATE %.2f%% STRATIFIED SAMPLE OF %s.%s ON %s", sampleRatio, get("data.table.schema"), get("data.table.name"), column);
                            AdaLogger.debug(this, "Stratified sample: " + sql);
                            getVerdict().sql(sql);
                        }
                        break;
                }
            }
            if (!get("sample.running.type").contains("uniform")) {
                getVerdict().sql(String.format("DROP %s%% UNIFORM SAMPLES OF %s.%s", get("sample.init.ratio"), get("data.table.schema"), get("data.table.name")));
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
