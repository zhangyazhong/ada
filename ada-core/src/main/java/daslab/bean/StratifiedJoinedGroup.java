package daslab.bean;

import java.io.Serializable;

/**
 * @author zyz
 * @version 2018-07-26
 */
public class StratifiedJoinedGroup implements Serializable {
    public String group_name;
    public long a_group_size;
    public long b_group_size;
    public long a_exchange_size;
    public long b_exchange_size;
    public double a_exchange_ratio;
    public double b_exchange_ratio;
    public double verdict_vprob;

    public StratifiedJoinedGroup() {
    }

    public StratifiedJoinedGroup(String group_name, long a_group_size, long b_group_size, long a_exchange_size, long b_exchange_size, double a_exchange_ratio, double b_exchange_ratio, double verdict_vprob) {
        this.group_name = group_name;
        this.a_group_size = a_group_size;
        this.b_group_size = b_group_size;
        this.a_exchange_size = a_exchange_size;
        this.b_exchange_size = b_exchange_size;
        this.a_exchange_ratio = a_exchange_ratio;
        this.b_exchange_ratio = b_exchange_ratio;
        this.verdict_vprob = verdict_vprob;
    }

    public String getGroup_name() {
        return group_name;
    }

    public void setGroup_name(String group_name) {
        this.group_name = group_name;
    }

    public long getA_group_size() {
        return a_group_size;
    }

    public void setA_group_size(long a_group_size) {
        this.a_group_size = a_group_size;
    }

    public long getB_group_size() {
        return b_group_size;
    }

    public void setB_group_size(long b_group_size) {
        this.b_group_size = b_group_size;
    }

    public long getA_exchange_size() {
        return a_exchange_size;
    }

    public void setA_exchange_size(long a_exchange_size) {
        this.a_exchange_size = a_exchange_size;
    }

    public long getB_exchange_size() {
        return b_exchange_size;
    }

    public void setB_exchange_size(long b_exchange_size) {
        this.b_exchange_size = b_exchange_size;
    }

    public double getA_exchange_ratio() {
        return a_exchange_ratio;
    }

    public void setA_exchange_ratio(double a_exchange_ratio) {
        this.a_exchange_ratio = a_exchange_ratio;
    }

    public double getB_exchange_ratio() {
        return b_exchange_ratio;
    }

    public void setB_exchange_ratio(double b_exchange_ratio) {
        this.b_exchange_ratio = b_exchange_ratio;
    }

    public double getVerdict_vprob() {
        return verdict_vprob;
    }

    public void setVerdict_vprob(double verdict_vprob) {
        this.verdict_vprob = verdict_vprob;
    }
}
