package daslab.exp5;

import daslab.exp.ExpResult;
import daslab.exp.ExpRunnable;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.List;

/**
 * @author zyz
 * @version 2018-10-14
 */
public class Exp5ResultErrorBound implements ExpRunnable {
    class ResultUnit {
        double r, e;
    }

    public final static String BASE_DIR = "/Users/zyz/Documents/AQP/paper/exp/exp15-check/";

    public final static String ACCURATE_RESULT_PATH = BASE_DIR + "accurate_result_24_48_1.csv";

    public final static String VERDICT_RESULT_PATH = BASE_DIR + "st_verdict_result_24_48_1.csv";
//    public final static String VERDICT_RESULT_PATH2 = BASE_DIR + "un+st2_verdict_result_24_48_1.csv";
    public final static String ADA_RESULT_PATH = BASE_DIR + "st_ada_result_24_48_1.csv";
    public final static String ADAPTIVE_RESULT_PATH = BASE_DIR + "st_adaptive_result_24_48_1.csv";

    public final static String COMPARISON_PATH = BASE_DIR + "error_bound_24_48_1.csv";

    private void approximateCompare() {
        ExpResult accurateResult = ExpResult.load(ACCURATE_RESULT_PATH);
        ExpResult adaResult = ExpResult.load(ADA_RESULT_PATH);
        ExpResult verdictResult = ExpResult.load(VERDICT_RESULT_PATH);
//        ExpResult verdictResult2 = ExpResult.load(VERDICT_RESULT_PATH2);
        ExpResult adaptiveResult = ExpResult.load(ADAPTIVE_RESULT_PATH);
        ExpResult comparisonResult = new ExpResult("time");
        assert accurateResult != null;
        assert adaResult != null;
        assert verdictResult != null;
//        assert verdictResult2 != null;
        assert adaptiveResult != null;
        try {
            for (String query : queries(accurateResult)) {
                for (String datetime : datetimes(accurateResult)) {
                    String accurateCell = accurateResult.getCell(datetime, query);
//                    String aggregationType = accurateCell.split("/")[0];
                    JSONObject accurateJSON = new JSONObject(StringUtils.substringBetween(accurateCell.split("/")[1], "[", "]"));
                    double accurate = accurateJSON.getDouble(accurateJSON.names().getString(0));
                    double relativeError = 0;
                    int repeatTime = 0;
                    for (String _query : verdictResult.getHeader()) {
                        if (_query.contains(query + "_")) {
                            repeatTime++;
                            String verdictCell = verdictResult.getCell(datetime, _query);
                            if (verdictCell == null) {
                                continue;
                            }
                            JSONObject verdictJSON = new JSONObject(StringUtils.substringBetween(verdictCell.split("/")[1], "[", "]"));
                            ResultUnit verdictUnit = fetch(verdictJSON);
                            relativeError = relativeError + verdictUnit.e;
                        }
                    }
                    /*
                    for (String _query : verdictResult2.getHeader()) {
                        if (_query.contains(query + "_")) {
                            repeatTime++;
                            String verdict2Cell = verdictResult2.getCell(datetime, _query);
                            JSONObject verdict2JSON = new JSONObject(StringUtils.substringBetween(verdict2Cell.split("/")[1], "[", "]"));
                            ResultUnit verdict2Unit = fetch(verdict2JSON);
                            relativeError = relativeError + verdict2Unit.e;
                        }
                    }
                    */
                    relativeError = relativeError / repeatTime / accurate;
                    comparisonResult.push(datetime, query + "_" + "verdict", String.format("%.8f", relativeError));
                    relativeError = 0;
                    repeatTime = 0;
                    for (String _query : adaResult.getHeader()) {
                        if (_query.contains(query + "_")) {
                            repeatTime++;
                            String adaCell = adaResult.getCell(datetime, _query);
                            JSONObject adaJSON = new JSONObject(StringUtils.substringBetween(adaCell.split("/")[1], "[", "]"));
                            ResultUnit adaUnit = fetch(adaJSON);
                            relativeError = relativeError + adaUnit.e;
                        }
                    }
                    relativeError = relativeError / repeatTime / accurate;
                    comparisonResult.push(datetime, query + "_" + "RRS", String.format("%.8f", relativeError));
                    relativeError = 0;
                    repeatTime = 0;
                    for (String _query : adaptiveResult.getHeader()) {
                        if (_query.contains(query + "_")) {
                            repeatTime++;
                            String adaptiveCell = adaptiveResult.getCell(datetime, _query);
                            JSONObject adaptiveJSON = new JSONObject(StringUtils.substringBetween(adaptiveCell.split("/")[1], "[", "]"));
                            ResultUnit adaptiveUnit = fetch(adaptiveJSON);
                            relativeError = relativeError + adaptiveUnit.e;
                        }
                    }
                    relativeError = relativeError / repeatTime / accurate;
                    comparisonResult.push(datetime, query + "_" + "adaptive", String.format("%.8f", relativeError));
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        comparisonResult.save(COMPARISON_PATH);
    }

    private List<String> queries(ExpResult accurateResult) {
        return accurateResult.getHeader().subList(1, accurateResult.getHeader().size());
    }

    private List<String> datetimes(ExpResult accurateResult) {
        return accurateResult.getRowKeys();
    }

    private ResultUnit fetch(JSONObject resultJSON) {
        ResultUnit resultUnit = new ResultUnit();
        try {
            for (int i = 0; i < resultJSON.names().length(); i++) {
                if (resultJSON.names().getString(i).contains("err")) {
                    resultUnit.e = resultJSON.getDouble(resultJSON.names().getString(i));
                } else {
                    resultUnit.r = resultJSON.getDouble(resultJSON.names().getString(i));
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return resultUnit;
    }

    @Override
    public void run() {
        approximateCompare();
    }
}
