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
        switch (sample.sampleType) {
            case "stratified":
                updateStratified(sample, adaBatch);
                break;
            case "uniform":
                updateUniform(sample, adaBatch);
                break;
        }
    }

    @Override
    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
        update(sample, adaBatch);
    }

    private void updateUniform(Sample sample, AdaBatch adaBatch) {
    }

    private void updateStratified(Sample sample, AdaBatch adaBatch) {

    }

    private long findX(Sample sample, AdaBatch adaBatch) {
        return 0L;
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
