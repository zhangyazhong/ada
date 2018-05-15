package dsalab.server;

import dsalab.context.AdaContext;
import dsalab.utils.AdaLogger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author zyz
 * @version 2018-05-11
 */
public class BatchReceiver {
    private AdaContext context;
    private String tmpDataLocation;
    private int serverPort;

    private BatchReceiver(AdaContext context) {
        this.context = context;
        tmpDataLocation = context.get("data.tmp.location");
        serverPort = Integer.parseInt(context.get("socket.server.port"));
    }

    public static BatchReceiver build(AdaContext context) {
        return new BatchReceiver(context);
    }

    public void start() {
        try {
            final ServerSocket server = new ServerSocket(serverPort);

            AdaLogger.info(this, String.format("Server has been started at %d.", serverPort));

            new Thread(() -> {
                while (true) {
                    try {
                        AdaLogger.info(this, "New batch arrived.");

                        Socket socket = server.accept();
                        receive(socket);

                        AdaLogger.info(this, String.format("New batch received. Stored at %s", tmpDataLocation));

                        context.afterOneBatch(tmpDataLocation);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void receive(Socket socket) {
        byte[] inputByte;
        int length;
        long totalSize = 0L;
        DataInputStream dataInputStream = null;
        FileOutputStream fileOutputStream = null;
        try {
            dataInputStream = new DataInputStream(socket.getInputStream());
            File file = new File(tmpDataLocation);
            if (!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            fileOutputStream = new FileOutputStream(file);
            inputByte = new byte[1024];
            while ((length = dataInputStream.read(inputByte, 0, inputByte.length)) > 0) {
                totalSize += (long) length;
                fileOutputStream.write(inputByte, 0, length);
                fileOutputStream.flush();
            }

            AdaLogger.info(this, String.format("Received size is %dMB.", totalSize / 1024L / 1024L));

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
                if (dataInputStream != null)
                    dataInputStream.close();
                if (socket != null)
                    socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
