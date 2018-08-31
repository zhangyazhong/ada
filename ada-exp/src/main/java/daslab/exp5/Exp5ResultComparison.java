package daslab.exp5;

import daslab.exp.ExpResult;
import daslab.exp.ExpRunnable;
import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import static daslab.exp.ExpConfig.HOUR_INTERVAL;
import static daslab.exp.ExpConfig.HOUR_START;
import static daslab.exp.ExpConfig.HOUR_TOTAL;

/**
 * @author zyz
 * @version 2018-08-31
 */
public class Exp5ResultComparison implements ExpRunnable {
    public final static String ACCURATE_SAVE_PATH = String.format("/tmp/ada/exp/exp5/accurate_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);
    public final static String ADA_SAVE_PATH = String.format("/tmp/ada/exp/exp5/ada_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);
    public final static String VERDICT_SAVE_PATH = String.format("/tmp/ada/exp/exp5/verdict_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);
    public final static String COMPARISON_SAVE_PATH = String.format("/tmp/ada/exp/exp5/comparison_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    @Override
    public void run() {
        ExpResult accurateResult = ExpResult.load(ACCURATE_SAVE_PATH);
        ExpResult adaResult = ExpResult.load(ADA_SAVE_PATH);
        ExpResult verdictResult = ExpResult.load(VERDICT_SAVE_PATH);
        ExpResult comparisonResult = new ExpResult("time");
        assert adaResult != null;
        assert verdictResult != null;
        assert accurateResult != null;
        try {
            for (String row : adaResult.getRowKeys()) {
                for (String column : adaResult.getHeader()) {
                    if (column.equals("time")) {
                        continue;
                    }
                    int queryNo = Integer.parseInt(StringUtils.substringBetween(column, "q", "_"));
                    String queryName = "q" + queryNo;
                    String accurateCell = accurateResult.getCell(row, queryName);
                    String adaCell = adaResult.getCell(row, column);
                    String verdictCell = verdictResult.getCell(row, column);
                    String aggregationType = accurateCell.split("/")[0];
                    JSONObject accurateJSON = new JSONObject(accurateCell.split("/")[1]);
                    JSONObject adaJSON = new JSONObject(adaCell.split("/")[1]);
                    JSONObject verdictJSON = new JSONObject(verdictCell.split("/")[1]);
                    switch (aggregationType) {
                        case "AVG":
                        case "SUM":
                        case "COUNT":
                            double accurate = accurateJSON.getDouble(accurateJSON.names().getString(0));
                            double adaR = 0, adaE = 0, verdictR = 0, verdictE = 0;
                            for (int i = 0; i < adaJSON.names().length(); i++) {
                                if (adaJSON.names().getString(i).contains("err")) {
                                    adaE = adaJSON.getDouble(adaJSON.names().getString(i));
                                } else {
                                    adaR = adaJSON.getDouble(adaJSON.names().getString(i));
                                }
                            }
                            for (int i = 0; i < verdictJSON.names().length(); i++) {
                                if (verdictJSON.names().getString(i).contains("err")) {
                                    verdictE = verdictJSON.getDouble(verdictJSON.names().getString(i));
                                } else {
                                    verdictR = verdictJSON.getDouble(verdictJSON.names().getString(i));
                                }
                            }
                            comparisonResult.push(row, queryName + "_ada", String.valueOf(
                                    comparisonResult.getCell(row, queryName + "_ada") != null ?
                                            Integer.parseInt(comparisonResult.getCell(row, queryName + "_ada")) : 0
                                            + Math.abs(adaR - accurate) <= adaE ? 1 : 0));
                            comparisonResult.push(row, queryName + "_verdict", String.valueOf(
                                    comparisonResult.getCell(row, queryName + "_verdict") != null ?
                                            Integer.parseInt(comparisonResult.getCell(row, queryName + "_verdict")) : 0
                                            + Math.abs(verdictR - accurate) <= verdictE ? 1 : 0));
                            break;
                    }
                }
            }
            comparisonResult.save(COMPARISON_SAVE_PATH);
        }
        catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
