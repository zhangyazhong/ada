package daslab.bean;

import java.io.Serializable;

public class SampleRowStatus implements Serializable {
    private long id;
    private int status;

    public SampleRowStatus() {
    }

    public SampleRowStatus(long id) {
        this.id = id;
        this.status = 1;
    }

    public SampleRowStatus(long id, int status) {
        this.id = id;
        this.status = status;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
