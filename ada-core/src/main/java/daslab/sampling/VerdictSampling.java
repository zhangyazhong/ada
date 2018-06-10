package daslab.sampling;

import daslab.bean.AdaBatch;
import daslab.bean.Sample;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;

import java.util.List;

/**
 * @author zyz
 * @version 2018-06-05
 */
public class VerdictSampling extends SamplingStrategy {
    public VerdictSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(AdaBatch adaBatch) {
        List<Sample> samples = getSamples();
        try {
            VerdictSpark2Context verdictSpark2Context = new VerdictSpark2Context(getContext().getDbmsSpark2().getSparkSession().sparkContext());
            AdaLogger.info(this, "About to drop all samples.");
            verdictSpark2Context.sql(String.format("DROP SAMPLES OF %s.%s",
                    getContext().get("dbms.default.database"), getContext().get("dbms.data.table")));
            for (Sample sample : samples) {
                AdaLogger.info(this, "About to create sample with sampling ratio " + sample.samplingRatio + " of " +  getContext().get("dbms.default.database") + "." + getContext().get("dbms.data.table"));
                verdictSpark2Context.sql("CREATE " + (sample.samplingRatio * 100) + "% UNIFORM SAMPLE OF " + getContext().get("dbms.default.database") + "." + getContext().get("dbms.data.table"));
            }
        } catch (VerdictException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String name() {
        return "verdict sampling";
    }
}
