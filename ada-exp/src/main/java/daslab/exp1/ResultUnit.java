package daslab.exp1;

/**
 * @author zyz
 * @version 2018-05-28
 */
public class ResultUnit {
    public double result;
    public double errorBound;

    public ResultUnit(double result) {
        this.result = result;
        this.errorBound = 0;
    }

    public ResultUnit(double result, double errorBound) {
        this.result = result;
        this.errorBound = errorBound;
    }
}
