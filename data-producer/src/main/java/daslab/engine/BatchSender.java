package daslab.engine;

import daslab.bean.Batch;
import daslab.context.ProducerContext;
import daslab.utils.AdaLogger;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zyz
 * @version 2018-05-09
 */
public class BatchSender {
    private ProducerContext context;
    private int timeDelay;
    private int timeInterval;
    private ScheduledExecutorService service;
    private String destHost;
    private int destPort;

    private BatchSender(ProducerContext context) {
        this.context = context;
        timeDelay = Integer.parseInt(context.get("producer.time.delay"));
        timeInterval = Integer.parseInt(context.get("producer.time.interval"));
        destHost = context.get("producer.dest.host");
        destPort = Integer.parseInt(context.get("producer.dest.port"));
    }

    public static BatchSender build(ProducerContext context) {
        return new BatchSender(context);
    }

    public void start() {
        service = Executors.newScheduledThreadPool(10);

        AdaLogger.info(this, String.format("Scheduler will be started in %ds with time interval %ds",
                timeDelay, timeInterval));

        service.scheduleAtFixedRate(
                () -> {
                    Batch batch = context.next();
                    if (send(batch)) {
                        context.mark();
                    }
                }, timeDelay, timeInterval, TimeUnit.SECONDS);
    }

    public boolean send() {
        return send(context.pop());
    }

    public boolean send(Batch batch) {
        AdaLogger.info(this, String.format("Sending no.%d batch to %s:%d", batch.getId(), destHost, destPort));

        int length;
        byte[] sendBytes;
        Socket socket = null;
        DataOutputStream dataOutputStream = null;
        FileInputStream fileInputStream = null;
        try {
            File file = batch.getDataFile();
            socket = new Socket();
            socket.connect(new InetSocketAddress(destHost, destPort));
            dataOutputStream = new DataOutputStream(socket.getOutputStream());
            fileInputStream = new FileInputStream(file);
            sendBytes = new byte[1024];
            while ((length = fileInputStream.read(sendBytes, 0, sendBytes.length)) > 0) {
                dataOutputStream.write(sendBytes, 0, length);
                dataOutputStream.flush();
            }
        } catch (Exception e) {
//            e.printStackTrace();
            return false;
        } finally{
            try {
                if (dataOutputStream != null)
                    dataOutputStream.close();
                if (fileInputStream != null)
                    fileInputStream.close();
                if (socket != null)
                    socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        AdaLogger.info(this, String.format("Successfully sent no.%d batch to %s:%d", batch.getId(), destHost, destPort));

        return true;
    }
}
