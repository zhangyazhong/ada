package daslab.bean;

import java.io.Serializable;

/**
 * @author zyz
 * @version 2018-09-25
 */
public class StratifiedAdaptiveGroup implements Serializable {
    public String group_name;
    public long u_group_size;
    public long v_group_size;
    public long group_size;
    public double verdict_vprob;

    public StratifiedAdaptiveGroup() {
    }

    public StratifiedAdaptiveGroup(String group_name, long u_group_size, long v_group_size, long group_size, double verdict_vprob) {
        this.group_name = group_name;
        this.u_group_size = u_group_size;
        this.v_group_size = v_group_size;
        this.group_size = group_size;
        this.verdict_vprob = verdict_vprob;
    }

    public String getGroup_name() {
        return group_name;
    }

    public void setGroup_name(String group_name) {
        this.group_name = group_name;
    }

    public long getU_group_size() {
        return u_group_size;
    }

    public void setU_group_size(long u_group_size) {
        this.u_group_size = u_group_size;
    }

    public long getV_group_size() {
        return v_group_size;
    }

    public void setV_group_size(long v_group_size) {
        this.v_group_size = v_group_size;
    }

    public double getVerdict_vprob() {
        return verdict_vprob;
    }

    public void setVerdict_vprob(double verdict_vprob) {
        this.verdict_vprob = verdict_vprob;
    }

    public long getGroup_size() {
        return group_size;
    }

    public void setGroup_size(long group_size) {
        this.group_size = group_size;
    }
}
