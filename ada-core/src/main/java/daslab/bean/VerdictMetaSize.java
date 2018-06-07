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
}
