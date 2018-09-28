package daslab.exp8;

import daslab.exp.ExpConfig;
import daslab.exp.ExpRunnable;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;

import java.io.*;

public class Exp8SplitTPCH implements ExpRunnable {
    public String inputFilePath = "/home/scidb/zyz/tpch/1000G/customer.tbl";
    public String outputFilePath = "/home/scidb/zyz/tpch/1000G/customer";

    @Override
    public void run() {
        try {
            inputFilePath = "/home/scidb/zyz/tpch/1000G/" + ExpConfig.get("file") + ".tbl";
            outputFilePath = "/home/scidb/zyz/tpch/1000G/" + ExpConfig.get("file");
            BufferedReader bufferedReader =
                    new BufferedReader(
                        new InputStreamReader(
                            new BufferedInputStream(
                                new FileInputStream(
                                    new File(inputFilePath)))),
                    32 * 1024 * 1024);
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFilePath));
            String row;
            String output;
            long n = 0;
            while ((row = bufferedReader.readLine()) != null) {
                if (row.length() > 1) {
                    n++;
                    if (n % 10000000 == 0) {
                        AdaLogger.info(this, "Finished " + n + " lines.");
                    }
                    output = StringUtils.substringBeforeLast(row, "|");
                    bufferedWriter.write(output);
                    bufferedWriter.newLine();
                }
            }
            bufferedWriter.flush();
            bufferedWriter.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
