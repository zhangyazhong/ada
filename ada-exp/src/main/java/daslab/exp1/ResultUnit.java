package daslab.exp1;

/**
 * @author zyz
 * @version 2018-05-28
 */
public class ResultUnit {
    public int time;
    public double result;
    public double errorBound;

    public ResultUnit(int time, double result) {
        this.time = time;
        this.result = result;
        this.errorBound = 0;
    }

    public ResultUnit(int time, double result, double errorBound) {
        this.time = time;
        this.result = result;
        this.errorBound = errorBound;
    }

    public void resultPlus(double result) {
        this.result = this.result + result;
    }

    @Override
    public String toString() {
        return String.format("{time: %d, result: %.2f, bound: %.2f}", time, result, errorBound);
    }
}
