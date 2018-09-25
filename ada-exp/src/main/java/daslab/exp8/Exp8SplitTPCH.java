package daslab.exp8;

import daslab.utils.AdaLogger;

import java.io.*;

public class Exp8SplitTPCH implements Runnable {
    public String inputFilePath = "/home/scidb/zyz/tpch/1000G/lineitem.tbl";
    public String outputFilePath = "/home/scidb/zyz/tpch/1000G/lineitem.csv";

    @Override
    public void run() {
        try {
            BufferedReader bufferedReader =
                    new BufferedReader(
                        new InputStreamReader(
                            new BufferedInputStream(
                                new FileInputStream(
                                    new File(inputFilePath)))),
                    32 * 1024 * 1024);
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFilePath));
            String row;
            String[] columns;
            String output;
            long n = 0;
            while ((row = bufferedReader.readLine()) != null) {
                if (row.length() > 1) {
                    n++;
                    if (n % 10000000 == 0) {
                        AdaLogger.info(this, "Finished " + n + " lines.");
                    }
                    columns = row.split("\\|");
                    output = columns[3] + "|" + columns[4] + "|" + columns[5] + "|" + columns[6] + "|" + columns[7] + "|" + columns[8];
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
