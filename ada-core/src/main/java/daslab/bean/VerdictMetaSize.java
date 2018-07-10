package daslab.bean;

/**
 * @author zyz
 * @version 2018-06-06
 */
public class VerdictMetaSize {
    public String schemaname;
    public String tablename;
    public long samplesize;
    public long originaltablesize;

    public VerdictMetaSize(String schemaname, String tablename, long samplesize, long originaltablesize) {
        this.schemaname = schemaname;
        this.tablename = tablename;
        this.samplesize = samplesize;
        this.originaltablesize = originaltablesize;
    }

    public String getSchemaname() {
        return schemaname;
    }

    public void setSchemaname(String schemaname) {
        this.schemaname = schemaname;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public long getSamplesize() {
        return samplesize;
    }

    public void setSamplesize(long samplesize) {
        this.samplesize = samplesize;
    }

    public long getOriginaltablesize() {
        return originaltablesize;
    }

    public void setOriginaltablesize(long originaltablesize) {
        this.originaltablesize = originaltablesize;
    }
}
