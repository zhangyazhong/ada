package daslab.sampling.strategy;

import daslab.bean.AdaBatch;
import daslab.bean.Sample;
import daslab.context.AdaContext;
import daslab.sampling.SamplingStrategy;

/**
 * @author zyz
 * @version 2018-05-15
 */
public class RandomUniformSampling extends SamplingStrategy {
    public RandomUniformSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(Sample sample, AdaBatch adaBatch) {

    }

    @Override
    public void update(Sample sample, AdaBatch adaBatch) {
        run(sample, adaBatch);
    }

    @Override
    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
    }

    @Override
    public String name() {
        return "random uniform";
    }

    @Override
    public String nameInPaper() {
        return "";
    }
}
