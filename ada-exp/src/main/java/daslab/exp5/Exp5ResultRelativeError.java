package daslab.exp5;

import daslab.exp.ExpResult;
import daslab.exp.ExpRunnable;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.List;

/**
 * @author zyz
 * @version 2018-10-12
 */
public class Exp5ResultRelativeError implements ExpRunnable {
    class ResultUnit {
        double r, e;
    }

    public final static String BASE_DIR = "/Users/zyz/Documents/AQP/paper/exp/exp19/";

    public final static String ACCURATE_RESULT_PATH = BASE_DIR + "accurate_result_20.csv";

//    public final static String NO_RESULT_PATH = BASE_DIR + "no_result_24_48_1.csv";
    public final static String VERDICT_RESULT_PATH = BASE_DIR + "resample_result_20.csv";
//    public final static String VERDICT_RESULT_PATH2 = BASE_DIR + "un+st2_verdict_result_24_48_1.csv";
//    public final static String VERDICT_RESULT_PATH = BASE_DIR + "ada_result_50.csv";
//    public final static String ADAPTIVE_RESULT_PATH = BASE_DIR + "adaptive_result_24_48_1.csv";
    public final static String UPDATE_RESULT_PATH = BASE_DIR + "update_result_20.csv";

    public final static String COMPARISON_PATH = BASE_DIR + "relative_error_20.csv";

    private void approximateCompare() {
//        ExpResult noResult = ExpResult.load(NO_RESULT_PATH);
        ExpResult accurateResult = ExpResult.load(ACCURATE_RESULT_PATH);
//        ExpResult adaResult = ExpResult.load(VERDICT_RESULT_PATH);
        ExpResult verdictResult = ExpResult.load(VERDICT_RESULT_PATH);
//        ExpResult verdictResult2 = ExpResult.load(VERDICT_RESULT_PATH2);
//        ExpResult adaptiveResult = ExpResult.load(ADAPTIVE_RESULT_PATH);
        ExpResult updateResult = ExpResult.load(UPDATE_RESULT_PATH);
        ExpResult comparisonResult = new ExpResult("time");
//        assert noResult != null;
        assert accurateResult != null;
//        assert adaResult != null;
        assert verdictResult != null;
//        assert verdictResult2 != null;
//        assert adaptiveResult != null;
        assert updateResult != null;
        try {
            for (String query : queries(accurateResult)) {
                for (String datetime : datetimes(accurateResult)) {
                    String accurateCell = accurateResult.getCell(datetime, query);
                    String aggregationType = accurateCell.split("/")[0];
                    if (aggregationType.contains("100")) {
                        aggregationType = "AVG";
                    }
                    JSONObject accurateJSON = new JSONObject(StringUtils.substringBetween(accurateCell.split("/")[1], "[", "]"));
                    double accurate = accurateJSON.getDouble(accurateJSON.names().getString(0));
                    double relativeError = 0;
                    int repeatTime = 0;

                    /*
                    for (String _query : noResult.getHeader()) {
//                        if (_query.contains("q" + (Integer.parseInt(StringUtils.substringAfter(query, "q")) + 24) + "_")) {
                        if (_query.contains(query + "_")) {
                            repeatTime++;
                            String noCell = noResult.getCell("00~00", _query);
                            JSONObject noJSON = new JSONObject(StringUtils.substringBetween(noCell.split("/")[1], "[", "]"));
                            ResultUnit noUnit = fetch(noJSON);
                            switch (aggregationType) {
                                case "AVG":
                                    relativeError = relativeError + Math.abs(noUnit.r - accurate) / accurate;
                                    break;
                                case "COUNT":
                                case "SUM":
                                default:
//                                    relativeError = relativeError + Math.abs(noUnit.r * (24 * (Integer.parseInt(datetime) / 100 - 1) + Integer.parseInt(datetime) % 100 + 1) / 24.0 - accurate) / accurate;
                                    relativeError = relativeError + Math.abs(noUnit.r * ((Integer.parseInt(datetime.split("~")[0]) + 1) * 40000000 + 3000028242.0) / 3000028242.0  - accurate) / accurate;
                                    break;
                            }
                        }
                    }
                    relativeError = relativeError / repeatTime;
                    comparisonResult.push(datetime, query + "_" + "no", String.format("%.4f", relativeError));
                    */


                    relativeError = 0;
                    repeatTime = 0;
                    for (String _query : verdictResult.getHeader()) {
//                        if (_query.contains("q") && ("q" + (Integer.valueOf(StringUtils.substringBetween(_query, "q", "_")) - 27) + "_").contains(query + "_")) {
                        if (_query.contains(query + "_")) {
                            repeatTime++;
                            String verdictCell = verdictResult.getCell(datetime, _query);
                            if (verdictCell == null) {
                                continue;
                            }
                            JSONObject verdictJSON = new JSONObject(StringUtils.substringBetween(verdictCell.split("/")[1], "[", "]"));
                            ResultUnit verdictUnit = fetch(verdictJSON);
                            relativeError = relativeError + Math.abs(Math.abs(verdictUnit.r - accurate) / accurate);
                        }
                    }


                    /*
                    for (String _query : verdictResult2.getHeader()) {
                        if (_query.contains(query + "_")) {
                            repeatTime++;
                            String verdict2Cell = verdictResult2.getCell(datetime, _query);
                            JSONObject verdict2JSON = new JSONObject(StringUtils.substringBetween(verdict2Cell.split("/")[1], "[", "]"));
                            ResultUnit verdict2Unit = fetch(verdict2JSON);
                            relativeError = relativeError + Math.abs(verdict2Unit.r - accurate) / accurate;
                        }
                    }
                    */


                    relativeError = relativeError / repeatTime;
                    comparisonResult.push(datetime, query + "_" + "resample", String.format("%.4f", relativeError));


                    /*
                    relativeError = 0;
                    repeatTime = 0;
                    for (String _query : adaResult.getHeader()) {
                        if (_query.contains(query + "_")) {
                            repeatTime++;
                            String adaCell = adaResult.getCell(datetime, _query);
                            if (adaCell == null) {
                                continue;
                            }
                            JSONObject adaJSON = new JSONObject(StringUtils.substringBetween(adaCell.split("/")[1], "[", "]"));
                            ResultUnit adaUnit = fetch(adaJSON);
                            relativeError = relativeError + Math.abs(adaUnit.r - accurate) / accurate;
                        }
                    }
                    relativeError = relativeError / repeatTime;
                    comparisonResult.push(datetime, query + "_" + "RRS", String.format("%.4f", relativeError));
                    */

                    /*
                    relativeError = 0;
                    repeatTime = 0;
                    for (String _query : adaptiveResult.getHeader()) {
                        if (_query.contains(query + "_")) {
                            repeatTime++;
                            String adaptiveCell = adaptiveResult.getCell(datetime, _query);
                            JSONObject adaptiveJSON = new JSONObject(StringUtils.substringBetween(adaptiveCell.split("/")[1], "[", "]"));
                            ResultUnit adaptiveUnit = fetch(adaptiveJSON);
                            relativeError = relativeError + Math.abs(adaptiveUnit.r - accurate) / accurate;
                        }
                    }
                    relativeError = relativeError / repeatTime;
                    comparisonResult.push(datetime, query + "_" + "adaptive", String.format("%.4f", relativeError));
                    */


                    relativeError = 0;
                    repeatTime = 0;
                    for (String _query : updateResult.getHeader()) {
                        if (_query.contains(query + "_")) {
//                        if (_query.contains("q") && ("q" + (Integer.valueOf(StringUtils.substringBetween(_query, "q", "_")) - 51) + "_").contains(query + "_")) {
                            repeatTime++;
                            String updateCell = updateResult.getCell(datetime, _query);
                            JSONObject updateJSON = new JSONObject(StringUtils.substringBetween(updateCell.split("/")[1], "[", "]"));
                            if (updateJSON.names() == null || updateJSON.names().length() < 1) {
                                repeatTime--;
                                continue;
                            }
                            ResultUnit updateUnit = fetch(updateJSON);
                            relativeError = relativeError + Math.abs(Math.abs(updateUnit.r - accurate) / accurate);
                        }
                    }
                    relativeError = relativeError / repeatTime;
                    comparisonResult.push(datetime, query + "_" + "update", String.format("%.4f", relativeError));


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
