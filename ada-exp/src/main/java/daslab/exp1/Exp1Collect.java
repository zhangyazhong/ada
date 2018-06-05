package daslab.exp1;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * @author zyz
 * @version 2018-06-05
 */
public class Exp1Collect {
    private final static String APPROXIMATE_RESULT_PATH = "/tmp/ada/exp/exp_app.csv";
    private final static String ACCURATE_RESULT_PATH = "/tmp/ada/exp/exp_acc.csv";
    private final static String HIT_PATH = "/tmp/ada/exp/exp_hit.csv";

    private Map<Integer, List<Integer>> hits;

    public Exp1Collect() {
        hits = Maps.newHashMap();
    }

    private boolean isInteger(String str) {
        Pattern pattern = Pattern.compile("^[-+]?[\\d]*$");
        return pattern.matcher(str).matches();
    }

    public void run() {
        try {
            BufferedReader approximateReader = new BufferedReader(new FileReader(APPROXIMATE_RESULT_PATH));
            BufferedReader accurateReader = new BufferedReader(new FileReader(ACCURATE_RESULT_PATH));
            Map<Integer, List<ResultUnit>> approximateResults = Maps.newHashMap();
            Map<Integer, List<ResultUnit>> accurateResults = Maps.newHashMap();
            String line;
            while ((line = approximateReader.readLine()) != null) {
                String[] parts = line.split(",");
                if (isInteger(parts[0])) {
                    int sampleNo = Integer.parseInt(parts[0]);
                    List<ResultUnit> results = Lists.newArrayList();
                    for (int i = 1; i <= 8; i++) {
                        results.add(new ResultUnit(24 * 7, Double.parseDouble(parts[i].split("/")[0]), Double.parseDouble(parts[i].split("/")[1])));
                    }
                    approximateResults.put(sampleNo, results);
                }
            }
            while ((line = accurateReader.readLine()) != null) {
                String[] parts = line.split(",");
                if (isInteger(parts[0])) {
                    int date = Integer.parseInt(parts[0]);
                    date = (date / 100) * 24 + date % 100 - 24;
                    List<ResultUnit> results = Lists.newArrayList();
                    for (int i = 1; i <= 8; i++) {
                        results.add(new ResultUnit(date, Double.parseDouble(parts[i])));
                    }
                    accurateResults.put(date, results);
                }
            }
            accurateResults.forEach((date, results0) -> {
                List<Integer> hitList = Lists.newArrayList();
                for (int i = 0; i < 8; i++) {
                    AtomicInteger hit = new AtomicInteger();
                    int finalI = i;
                    approximateResults.forEach((no, results1) -> {
                        if (finalI < 4) {
                            if (Math.abs(results1.get(finalI).result - results0.get(finalI).result) <= results1.get(finalI).errorBound) {
                                hit.getAndIncrement();
                            }
                        } else {
//                            if (Math.abs(results1.get(finalI).result * results0.get(finalI).time / results1.get(finalI).time - results0.get(finalI).result) <= results1.get(finalI).errorBound) {
                            if (Math.abs(results1.get(finalI).result - results0.get(finalI).result * results1.get(finalI).time / results0.get(finalI).time) <= results1.get(finalI).errorBound) {
                                hit.getAndIncrement();
                            }
                        }
                    });
                    hitList.add(hit.get());
                }
                hits.put(date, hitList);
            });
            save();
            approximateReader.close();
            accurateReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void save() {
        try {
            FileWriter fileWriter = new FileWriter(new File(HIT_PATH));
            StringBuilder header = new StringBuilder("date");
            for (int i = 0; i < 8; i++) {
                header.append(",q").append(i + 1);
            }
            header.append("\r\n");
            fileWriter.write(header.toString());
            hits.forEach((date, results) -> {
                try {
                    String time = String.format("%02d%02d", (date / 24) + 1, date % 24);
                    StringBuilder body = new StringBuilder(time);
                    for (int i = 0; i < 8; i++) {
                        body.append(",").append(results.get(i));
                    }
                    body.append("\r\n");
                    fileWriter.write(body.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Exp1Collect exp1Collect = new Exp1Collect();
        exp1Collect.run();
    }
}
