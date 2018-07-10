package daslab.bean;

import java.io.Serializable;

public class PageCountSampleWithStatus implements Serializable {
    public int date_time;
    public String project_name;
    public String page_name;
    public int page_count;
    public int page_size;
    public double verdict_rand;
    public double verdict_vpart;
    public double verdict_vprob;
    public int status;

    public PageCountSampleWithStatus() {
    }

    public PageCountSampleWithStatus(int date_time, String project_name, String page_name, int page_count, int page_size, double verdict_rand, double verdict_vpart, double verdict_vprob, int status) {
        this.date_time = date_time;
        this.project_name = project_name;
        this.page_name = page_name;
        this.page_count = page_count;
        this.page_size = page_size;
        this.verdict_rand = verdict_rand;
        this.verdict_vpart = verdict_vpart;
        this.verdict_vprob = verdict_vprob;
        this.status = status;
    }

    public int getDate_time() {
        return date_time;
    }

    public void setDate_time(int date_time) {
        this.date_time = date_time;
    }

    public String getProject_name() {
        return project_name;
    }

    public void setProject_name(String project_name) {
        this.project_name = project_name;
    }

    public String getPage_name() {
        return page_name;
    }

    public void setPage_name(String page_name) {
        this.page_name = page_name;
    }

    public int getPage_count() {
        return page_count;
    }

    public void setPage_count(int page_count) {
        this.page_count = page_count;
    }

    public int getPage_size() {
        return page_size;
    }

    public void setPage_size(int page_size) {
        this.page_size = page_size;
    }

    public double getVerdict_rand() {
        return verdict_rand;
    }

    public void setVerdict_rand(double verdict_rand) {
        this.verdict_rand = verdict_rand;
    }

    public double getVerdict_vpart() {
        return verdict_vpart;
    }

    public void setVerdict_vpart(double verdict_vpart) {
        this.verdict_vpart = verdict_vpart;
    }

    public double getVerdict_vprob() {
        return verdict_vprob;
    }

    public void setVerdict_vprob(double verdict_vprob) {
        this.verdict_vprob = verdict_vprob;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
