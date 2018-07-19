package daslab.exp4;

import com.google.common.collect.Lists;
import daslab.exp.ExpResult;
import daslab.exp.ExpRunnable;
import daslab.utils.AdaLogger;

import java.io.*;
import java.util.List;

public class Exp4Comparison implements ExpRunnable {
    private final static String VERDICT_PATH = "/tmp/ada/exp/exp4/exp4_verdict";
    private final static String ADA_PATH = "/tmp/ada/exp/exp4/exp4_ada";
    private final static String ACCURATE_PATH = "/tmp/ada/exp/exp4/exp4_accurate";

    private ExpResult load(String path) {
        ExpResult expResult = new ExpResult();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(path)));
            String[] header = bufferedReader.readLine().split(",");
            expResult.setHeader(header);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] row = line.split(",");
                for (int i = 1; i < row.length; i++) {
                    expResult.addResult(row[0], row[i]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return expResult;
    }

    @Override
    public void run() {
        ExpResult adaResult = load(ADA_PATH);
        ExpResult verdictResult = load(VERDICT_PATH);
        ExpResult accurateResult = load(ACCURATE_PATH);
        ExpResult summaryResult = new ExpResult();
        List<String> queries = accurateResult.getHeader();
        List<String> verdictHeader = verdictResult.getHeader();
        List<String> adaHeader = adaResult.getHeader();
        List<String> summaryHeader = Lists.newLinkedList();
        summaryHeader.add("time");
        for (int k = 1; k < queries.size(); k++) {
            summaryHeader.add(queries.get(k) + "_verdict");
            summaryHeader.add(queries.get(k) + "_ada");
        }
        summaryResult.setHeader(summaryHeader);
        for (int k = 1; k < queries.size(); k++) {
            String query = queries.get(k);
            for (String key : accurateResult.getRowKeys()) {
                double accurate = 0;
                for (int i = 1; i < queries.size(); i++) {
                    if (queries.get(i).contains(query)) {
                        accurate = Double.parseDouble(accurateResult.getColumns(key).get(i - 1));
                    }
                }
                AdaLogger.info(this, String.format("%s: %.8f", key, accurate));
                int verdictHit = 0;
                for (int i = 1; i < verdictHeader.size(); i++) {
                    if (verdictHeader.get(i).contains(query)) {
                        String[] result = verdictResult.getColumns(key).get(i - 1).split("/");
                        double verdict = Double.parseDouble(result[0]);
                        double err = Double.parseDouble(result[1]);
                        AdaLogger.info(this, String.format("[%s][q%d][No.%d]: %.8f/%.8f", key, k, i, verdict, err));
                        if (Math.abs(accurate - verdict) <= err) {
                            verdictHit++;
                        }
                    }
                }
                summaryResult.addResult(key, String.valueOf(verdictHit));
                int adaHit = 0;
                for (int i = 1; i < adaHeader.size(); i++) {
                    if (adaHeader.get(i).contains(query)) {
                        String[] result = adaResult.getColumns(key).get(i - 1).split("/");
                        double ada = Double.parseDouble(result[0]);
                        double err = Double.parseDouble(result[1]);
                        AdaLogger.info(this, String.format("[%s][q%d][No.%d]: %.8f/%.8f", key, k, i, ada, err));
                        if (Math.abs(accurate - ada) <= err) {
                            adaHit++;
                        }
                    }
                }
                summaryResult.addResult(key, String.valueOf(adaHit));
            }
        }
        summaryResult.save("/tmp/ada/exp/exp4/exp4_summary");
    }
}
