package daslab.exp4;

import daslab.exp.ExpResult;
import org.apache.commons.lang.StringUtils;

import java.io.*;

public class Exp4Comparison {
    private final static String VERDICT_PATH = "/tmp/ada/exp/exp4/exp4_verdict";
    private final static String ADA_PATH = "/tmp/ada/exp/exp4/exp4_ada";
    private final static String ACCURATE_PATH = "/tmp/ada/exp/exp4/exp4_accurate";

    public Exp4Comparison() {

    }

    private ExpResult load(String path) {
        ExpResult expResult = new ExpResult();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(path)));
            String[] header = bufferedReader.readLine().split(",");
            expResult.setHeader(header);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return expResult;
    }
}
