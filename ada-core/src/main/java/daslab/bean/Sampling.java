package daslab.bean;

/**
 * @author zyz
 * @version 2018-06-05
 */
public enum Sampling {
    RESAMPLE("resample"), UPDATE("update");

    private String strategy;

    Sampling(String strategy) {
        this.strategy = strategy;
    }

    @Override
    public String toString() {
        return strategy;
    }
}
