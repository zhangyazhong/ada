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

    public String getOriginalschemaname() {
        return originalschemaname;
    }

    public void setOriginalschemaname(String originalschemaname) {
        this.originalschemaname = originalschemaname;
    }

    public String getOriginaltablename() {
        return originaltablename;
    }

    public void setOriginaltablename(String originaltablename) {
        this.originaltablename = originaltablename;
    }

    public String getSampleschemaaname() {
        return sampleschemaaname;
    }

    public void setSampleschemaaname(String sampleschemaaname) {
        this.sampleschemaaname = sampleschemaaname;
    }

    public String getSampletablename() {
        return sampletablename;
    }

    public void setSampletablename(String sampletablename) {
        this.sampletablename = sampletablename;
    }

    public String getSampletype() {
        return sampletype;
    }

    public void setSampletype(String sampletype) {
        this.sampletype = sampletype;
    }

    public double getSamplingratio() {
        return samplingratio;
    }

    public void setSamplingratio(double samplingratio) {
        this.samplingratio = samplingratio;
    }

    public String getColumnnames() {
        return columnnames;
    }

    public void setColumnnames(String columnnames) {
        this.columnnames = columnnames;
    }

    @Override
    public String toString() {
        return "VerdictMetaName{" +
                "originalschemaname='" + originalschemaname + '\'' +
                ", originaltablename='" + originaltablename + '\'' +
                ", sampleschemaaname='" + sampleschemaaname + '\'' +
                ", sampletablename='" + sampletablename + '\'' +
                ", sampletype='" + sampletype + '\'' +
                ", samplingratio=" + samplingratio +
                ", columnnames='" + columnnames + '\'' +
                '}';
    }
}
