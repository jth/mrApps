package de.jth.ma.wc;

import com.google.gson.Gson;
import de.jth.ma.wc.Messages.MapperFinishedMsg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jth on 12/14/15.
 */
public class Reducer {
    public void startServer() {
        final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);
        System.out.println("Starting server to accept responses");

        Runnable serverTask = new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(ApplicationMasterAsync.REDUCER_PORT);
                    System.out.println("Reducer: Waiting for clients to connect...");
                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        clientProcessingPool.submit(new ClientTask(clientSocket));
                    }
                } catch (IOException e) {
                    System.err.println("Reducer: Unable to process client request");
                    e.printStackTrace();
                }
            }
        };
        Thread serverThread = new Thread(serverTask);
        serverThread.start();
    }

    private class ClientTask implements Runnable {
        private final Socket clientSocket;

        private ClientTask(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            System.out.println("Reducer: Message from " + clientSocket.toString());

            BufferedReader br;
            StringBuffer inMsg = new StringBuffer();
            String line;

            try {
                br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                while ((line = br.readLine()) != null) {
                    inMsg.append(line);
                }
            } catch (IOException e) {
                throw new RuntimeException("Could not read MapperFinishedMsg: " + e.toString());
            }
            final Gson gson = new Gson();
            final MapperFinishedMsg msg = gson.fromJson(inMsg.toString(), MapperFinishedMsg.class);
            System.out.println("Reducer: Received finished Mapper Msg: " + msg.toString());

            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public Reducer() {

    }

    public static void main(String args[]) {

    }
}
