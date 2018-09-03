package daslab.sampling.strategy;

import daslab.bean.AdaBatch;
import daslab.bean.Sample;
import daslab.context.AdaContext;
import daslab.sampling.SamplingStrategy;

@SuppressWarnings("Duplicates")
public class AdaptiveSampling extends SamplingStrategy {
    public AdaptiveSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(Sample sample, AdaBatch adaBatch) {
    }

    @Override
    public void update(Sample sample, AdaBatch adaBatch) {
    }

    @Override
    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
        switch (sample.sampleType) {
            case "stratified":
                resampleStratified(sample, adaBatch);
                break;
            case "uniform":
                resampleUniform(sample, adaBatch);
                break;
        }
    }

    private void resampleUniform(Sample sample, AdaBatch adaBatch) {

    }

    private void resampleStratified(Sample sample, AdaBatch adaBatch) {

    }

    @Override
    public String name() {
        return "adaptive";
    }

    @Override
    public String nameInPaper() {
        return "ARS";
    }
}
