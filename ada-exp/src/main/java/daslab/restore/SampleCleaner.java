package daslab.restore;

import daslab.exp.ExpTemplate;
import org.apache.spark.sql.Row;

import java.util.List;

public class SampleCleaner extends ExpTemplate implements RestoreModule {

    public SampleCleaner() {
        this("Ada Exp - DatabaseRestore");
    }

    public SampleCleaner(String name) {
        super(name);
    }

    @Override
    public void run() {
        restore();
    }

    @Override
    public void restore() {
        List<Row> databases = getSpark().sql("SHOW DATABASES").collectAsList();
        for (Row database : databases) {
            String name = database.getString(0);
            if (name.contains("verdict")) {
                execute("DROP DATABASE " + name + " CASCADE");
            }
        }
    }

    public static void main(String[] args) {
        RestoreModule r = new SampleCleaner();
        r.restore();
    }
}
