package de.jth.ma.wc;

import com.google.gson.Gson;
import de.jth.ma.wc.Messages.MapperFinishedMsg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by jth on 10/6/15.
 */
public class Mapper {
    private final Path inputFile;
    private final String outputPath;
    private final int id;
    private final int taskDuration;
    private final Map<String, Long> counts = new HashMap<>();
    final static int APPMASTER_PORT = 2323;
    private final static String APPMASTER_HOST = "asct-phobos";

    Mapper(Path inputFile, String outputPath, int id, int taskDuration) {
        this.inputFile = inputFile;
        this.outputPath = outputPath;
        this.id = id;
        this.taskDuration = taskDuration;
    }

    private void writeResult(FileSystem fs) throws IOException {
        Path out = new Path(outputPath + "_" + id);
        final FSDataOutputStream os = fs.create(out, true);

        Iterator it = counts.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            String result = pair.getKey() + "\t" + pair.getValue() + "\n";
            os.write(result.getBytes());
            it.remove(); // avoids a ConcurrentModificationException
        }

        os.close();
    }

    private ServerSocket listenToPull() {
        ServerSocket s = null;
        try {
            s = new ServerSocket(0);
        } catch (IOException e) {
            throw new RuntimeException("Could not open listening port for result pulling: " + e.getMessage());
        }
        System.out.println("listening on port for result sharing: " + s.getLocalPort());
        return s;
    }

    private void sendResult(Socket s) {
        File f = new File(outputPath + "_" + id);
        byte[] bytes = new byte[16 * 1024];
        InputStream in = null;
        OutputStream out = null;
        try {
            in = new FileInputStream(f);
            out = s.getOutputStream();
        } catch (IOException e) {
            throw new RuntimeException("Could not get output stream from socket: " + e.getMessage());
        }
        int count;
        try {
            while ((count = in.read(bytes)) > 0) {
                out.write(bytes, 0, count);
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not read file: " + e.getMessage());
        }
    }

    private void signalFinishAndWaitForPull() {
        final ServerSocket serverSocket = listenToPull();
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Could not determine hostname: " + e.getMessage());
        }
        final MapperFinishedMsg mapperFinishedMsg = new MapperFinishedMsg(id, serverSocket.getLocalPort(), hostname);
        try {
            final Socket sendSocket = new Socket(APPMASTER_HOST, APPMASTER_PORT);
            final Gson gson = new Gson();
            final String finishedJsonMsg = gson.toJson(mapperFinishedMsg);
            final OutputStream os = sendSocket.getOutputStream();
            final OutputStreamWriter osw = new OutputStreamWriter(os);
            final BufferedWriter bw = new BufferedWriter(osw);
            bw.write(finishedJsonMsg);
            bw.flush();
            System.out.println("Sent finished Message to AppMaster " + APPMASTER_HOST + ":" + APPMASTER_PORT);
            bw.close();
            sendSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Could not open socket to AppMaster " + APPMASTER_HOST + ":" + APPMASTER_PORT);
        }
        try {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Received pull from " + clientSocket.toString());
            sendResult(clientSocket);
            clientSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("accept() on ServerSocket failed: " + e.getMessage());
        }

    }

    void exec() {
        try{
            final FileSystem fs = FileSystem.get(new Configuration());
            final FSDataInputStream is = fs.open(inputFile);
            final BufferedReader br = new BufferedReader(new InputStreamReader(is.getWrappedStream()));
            String line = br.readLine();

            while (line != null){
                String[] words = line.split("\\s+");
                for (String word : words) {
                    if (!counts.containsKey(word)) {
                        counts.put(word, new Long(1));
                    } else {
                        counts.put(word, counts.get(word) + 1);
                    }
                }
                //Thread.sleep(WAIT_TIME); // prolong tasks artificially...
                line = br.readLine();
            }
            // TODO: Remove non printable characters. Do that while reading to avoid multiple spaces, tabs and so on. But ok for now.
            counts.remove("\t");
            //System.out.println("Counted words, closing " + inputFile.getName());
            is.close();
            System.out.println("Writing results");
            writeResult(fs);
            // Send finish result to AppMaster and wait for pull
        }catch(Exception e){
            throw new RuntimeException("Could not open " + inputFile.getName() + ": " + e.getMessage());
        }
        signalFinishAndWaitForPull();
    }

    public static void main(String args[]) {
        System.out.println("Instantiating mapTask");
        final Mapper mapTask = new Mapper(new Path(args[0]), args[1], Integer.valueOf(args[2]), Integer.valueOf(args[3]));
        mapTask.exec();
    }
}
