package daslab.exp5;

import daslab.exp.ExpResult;
import daslab.exp.ExpRunnable;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author zyz
 * @version 2018-08-31
 */
public class Exp5ResultComparison implements ExpRunnable {
    /*
    public final static String ACCURATE_RESULT_PATH = String.format("/tmp/ada/exp/exp5/accurate_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);
    public final static String ADA_RESULT_PATH = String.format("/tmp/ada/exp/exp5/ada_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);
    public final static String VERDICT_RESULT_PATH = String.format("/tmp/ada/exp/exp5/verdict_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);
    public final static String COMPARISON_SAVE_PATH = String.format("/tmp/ada/exp/exp5/comparison_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);
    */
    public final static String BASE_DIR = "/Users/zyz/Documents/AQP/paper/exp/exp15-check/";

    public final static String ACCURATE_RESULT_PATH = BASE_DIR + "accurate_result_24_48_1.csv";

    public final static String ADA_RESULT_PATH = BASE_DIR + "st_ada_result_24_48_1.csv";
    public final static String ADA_COMPARISON_PATH = BASE_DIR + "compare_ada_24_48_1.csv";

    public final static String VERDICT_RESULT_PATH = BASE_DIR + "st_verdict_result_24_48_1.csv";
    public final static String VERDICT_COMPARISON_PATH = BASE_DIR + "compare_verdict_24_48_1.csv";

    public final static String ADAPTIVE_RESULT_PATH = BASE_DIR + "st_adaptive_result_24_48_1.csv";
    public final static String ADAPTIVE_COMPARISON_PATH = BASE_DIR + "compare_adaptive_24_48_1.csv";

    private void compareVerdict() {
        approximateCompare(VERDICT_RESULT_PATH, VERDICT_COMPARISON_PATH);
    }

    private void compareAda() {
        approximateCompare(ADA_RESULT_PATH, ADA_COMPARISON_PATH);
    }

    private void compareAdaptive() {
        approximateCompare(ADAPTIVE_RESULT_PATH, ADAPTIVE_COMPARISON_PATH);
    }

    private void approximateCompare(String approximateResultPath, String approximateComparisonPath) {
        ExpResult accurateResult = ExpResult.load(ACCURATE_RESULT_PATH);
        ExpResult approximateResult = ExpResult.load(approximateResultPath);
        ExpResult comparisonResult = new ExpResult("time");
        assert approximateResult != null;
        assert accurateResult != null;
        try {
            for (String row : approximateResult.getRowKeys()) {
                for (String column : approximateResult.getHeader()) {
                    if (column.equals("time")) {
                        continue;
                    }
                    int queryNo = Integer.parseInt(StringUtils.substringBetween(column, "q", "_"));
//                    if (Integer.parseInt(column.substring(column.length() - 1)) > 4) {
//                        continue;
//                    }
                    String queryName = "q" + queryNo;
                    String approximateCell = approximateResult.getCell(row, column);
                    String accurateCell = accurateResult.getCell(row, queryName);
                    System.out.println(row + " " + queryName + " " + column + " " + approximateCell + " " + accurateCell);
                    String aggregationType = accurateCell.split("/")[0];
                    JSONObject accurateJSON = new JSONObject(StringUtils.substringBetween(accurateCell.split("/")[1],"[", "]"));
                    JSONObject approximateJSON = new JSONObject(StringUtils.substringBetween(approximateCell.split("/")[1], "[", "]"));
                    switch (aggregationType) {
                        case "AVG":
                        case "SUM":
                        case "COUNT":
                            double accurate = accurateJSON.getDouble(accurateJSON.names().getString(0));
                            double approximateR = 0, approximateE = 0;
                            for (int i = 0; i < approximateJSON.names().length(); i++) {
                                if (approximateJSON.names().getString(i).contains("err")) {
                                    approximateE = approximateJSON.getDouble(approximateJSON.names().getString(i));
                                } else {
                                    approximateR = approximateJSON.getDouble(approximateJSON.names().getString(i));
                                }
                            }
                            comparisonResult.push(row, queryName, String.valueOf(
                                    comparisonResult.getCell(row, queryName) != null ?
                                            Integer.parseInt(comparisonResult.getCell(row, queryName)) + (Math.abs(approximateR - accurate) <= approximateE ? 1 : 0) : 0));
                            break;
                    }
                }
            }
            comparisonResult.save(approximateComparisonPath);
        }
        catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
//        compareAda();
//        compareVerdict();
        compareAdaptive();
    }
}
