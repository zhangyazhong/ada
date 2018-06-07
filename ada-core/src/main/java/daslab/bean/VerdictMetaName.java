package daslab.bean;

/**
 * @author zyz
 * @version 2018-06-06
 */
public class VerdictMetaName {
    public String originalschemaname;
    public String originaltablename;
    public String sampleschemaaname;
    public String sampletablename;
    public String sampletype;
    public double samplingratio;
    public String columnnames;

    public VerdictMetaName(String originalschemaname, String originaltablename, String sampleschemaaname, String sampletablename, String sampletype, double samplingratio, String columnnames) {
        this.originalschemaname = originalschemaname;
        this.originaltablename = originaltablename;
        this.sampleschemaaname = sampleschemaaname;
        this.sampletablename = sampletablename;
        this.sampletype = sampletype;
        this.samplingratio = samplingratio;
        this.columnnames = columnnames;
    }
}
