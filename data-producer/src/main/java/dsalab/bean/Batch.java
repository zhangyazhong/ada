package dsalab.bean;

import java.io.File;

/**
 * @author zyz
 * @version 2018-05-10
 */
public class Batch {
    private static int TOTAL;
    private int id;
    private File dataFile;

    static {
        TOTAL = 0;
    }

    public Batch() {
        id = TOTAL;
        TOTAL += 1;
    }
    public Batch(File dataFile) {
        this();
        this.dataFile = dataFile;
    }

    public int getId() {
        return id;
    }

    public File getDataFile() {
        return dataFile;
    }

    public void setDataFile(File dataFile) {
        this.dataFile = dataFile;
    }
}
