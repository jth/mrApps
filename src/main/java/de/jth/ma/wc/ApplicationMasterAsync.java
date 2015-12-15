package de.jth.ma.wc;

/**
 * Created by jth on 10/6/15.
 */


import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import de.jth.ma.wc.Messages.MapperFinishedMsg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {
    public final static int REDUCER_PORT = 4242;
    private final AtomicInteger PARALLEL_CONTAINERS = new AtomicInteger();
    private final AtomicInteger NUM_REDUCERS = new AtomicInteger(1); // max number of reducers
    private final AtomicBoolean increased1 = new AtomicBoolean(false);
    private final AtomicBoolean increased2 = new AtomicBoolean(false);
    private final AtomicInteger reducersSpawned = new AtomicInteger(0);
    private static final long DEADLINE = 16000; // Deadline in ms for whole job completion
    private int estimatedDeadline = 0; // Estimated job finish
    private final Path inputPath;
    private final Path outputPath;
    private final int splits;
    private final List<NodeId> availableNodes;
    private final List<ContainerRequest> requestList = new ArrayList<>();
    private final List<ContainerId> finishedContainers = new ArrayList<>();
    private final Map<ContainerId, TimeTuple> containerTimes = new HashMap<>();
    private final List<MapperFinishedMsg> finishedMapper = new CopyOnWriteArrayList<>();
    private final AppMasterConfig appMasterConfig;
    private List<Container> allocatedContainers = new ArrayList<>();
    private YarnConfiguration configuration;
    private NMClientAsync nmClient;
    private AMRMClientAsync<ContainerRequest> rmClient;
    private InputSplitter splitter;
    // Counters, use atomic integer here?
    private final AtomicInteger completedContainers = new AtomicInteger(0);
    private int ctr = 0;
    private int absoluteContainersRequested = 0; // How many containers where requested overall during job?
    private int containersRequested = 0; // Containers requests pending
    private int containersToWaitFor;
    private int runningContainers = 0;

    public void startServer() {
        final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);
        System.out.println("Starting server to accept responses");

        Runnable serverTask = new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(Mapper.APPMASTER_PORT);
                    System.out.println("Waiting for clients to connect...");
                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        clientProcessingPool.submit(new ClientTask(clientSocket, appMasterConfig));
                    }
                } catch (IOException e) {
                    System.err.println("Unable to process client request");
                    e.printStackTrace();
                }
            }
        };
        Thread serverThread = new Thread(serverTask);
        serverThread.start();
    }

    private class ClientTask implements Runnable {
        private final Socket clientSocket;
        private final AppMasterConfig appMasterConfig;

        private ClientTask(Socket clientSocket, AppMasterConfig appMasterConfig) {
            this.clientSocket = clientSocket;
            this.appMasterConfig = appMasterConfig;
        }

        @Override
        public void run() {
            System.out.println("Message from " + clientSocket.toString());

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
            System.out.println("Received msg from mapper: " + msg.toString());
            finishedMapper.add(msg);

            if (finishedMapper.size() < appMasterConfig.initialContainers) {
                ApplicationMasterAsync.this.requestContainers(appMasterConfig.initialContainers - finishedMapper.size());
            }

            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {

    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

    }

    @Override
    public void onContainerStopped(ContainerId containerId) {

    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable throwable) {

    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {

    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable throwable) {

    }

    private static class TimeTuple {
        public final long startTime;
        public long endTime = 0;

        TimeTuple(long startTime) {
            this.startTime = startTime;
        }

        @Override
        public String toString() {
            return "startTime: " + startTime + ", endTime: " + endTime;
        }
    }

    AppMasterConfig readConfig(String filename) {
        Gson gson = new Gson();
        InputStream in = getClass().getResourceAsStream("/" + filename);
        BufferedReader input = new BufferedReader(new InputStreamReader(in));
        return gson.fromJson(new JsonReader(input), AppMasterConfig.class);
    }

    public ApplicationMasterAsync(Path inputPath, Path outputPath, List<NodeId> availableNodes) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.availableNodes = availableNodes;
        this.appMasterConfig = readConfig("appmaster.conf");
        System.out.println("Using config: " + appMasterConfig.toString());
        PARALLEL_CONTAINERS.set(appMasterConfig.initialContainers);
        splitter = new InputSplitter(inputPath);
        try {
            splitter.stat();
        } catch (IOException e) {
            throw new RuntimeException("Could not split input: " + e.getMessage());
        }
        splits = containersToWaitFor = splitter.getStats().size();
        startServer();
        configuration = new YarnConfiguration();
        nmClient = NMClientAsync.createNMClientAsync(this);
        nmClient.init(configuration);
        nmClient.start();
        System.out.println("Available Nodes: ");
        for (NodeId nodeId : availableNodes) {
            System.out.println(nodeId.toString());
        }
    }

    private LocalResource setupLocalResource() {
        try {
            FileStatus status = FileSystem.get(configuration).getFileStatus(Client.jarPathHdfs);
            URL packageUrl = ConverterUtils.getYarnUrlFromPath(
                    FileContext.getFileContext().makeQualified(Client.jarPathHdfs));

            LocalResource packageResource = Records.newRecord(LocalResource.class);
            packageResource.setResource(packageUrl);
            packageResource.setSize(status.getLen());
            packageResource.setTimestamp(status.getModificationTime());
            packageResource.setType(LocalResourceType.ARCHIVE);
            packageResource.setVisibility(LocalResourceVisibility.APPLICATION);

            return packageResource;
        } catch (IOException e) {
            throw new RuntimeException("Could not add local resource: " + e.getMessage());
        }
    }

    private void executeReducer(Container container) {
        final String command =
                "$JAVA_HOME/bin/java" +
                        " -Xmx256M" +
                        " de.jth.ma.wc.Reducer" +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        try {
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
            ctx.setLocalResources(Collections.singletonMap("package", setupLocalResource()));
            ctx.setCommands(Collections.singletonList(command));
            System.out.println("[AM] Launching container (reducer): " + container.getId());
            synchronized (this) {
                containerTimes.put(container.getId(), new TimeTuple(System.currentTimeMillis()));
                ++runningContainers;
                ++ctr;
                nmClient.startContainerAsync(container, ctx);
            }
            System.out.println("executeReducer: running Containers: " + runningContainers);
        } catch (Exception ex) {
            System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
        }
    }

    private void executeMapper(Container container) {
        final String command =
                "$JAVA_HOME/bin/java" +
                        " -Xmx128M" +
                        //" -cp './package/*'" +
                        " de.jth.ma.wc.Mapper" +
                        " '" + splitter.getStats().get(ctr).getPath().toString() + // use ' to fix issues with special characters
                        "' " + outputPath +
                        " " + ctr +
                        " " + appMasterConfig.taskDuration +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        try {
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
            ctx.setLocalResources(Collections.singletonMap("package", setupLocalResource()));
            ctx.setCommands(Collections.singletonList(command));
            System.out.println("[AM] Launching container (mapper): " + container.getId());

            synchronized (this) {
                containerTimes.put(container.getId(), new TimeTuple(System.currentTimeMillis()));
                ++runningContainers;
                ++ctr;
                nmClient.startContainerAsync(container, ctx);
            }
            System.out.println("executeMapper: running Containers: " + runningContainers);
        } catch (Exception ex) {
            System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
        }
    }

    public void onContainersAllocated(List<Container> containers) {
        System.out.println("Before onContainersAllocated: running / requested / waiting for: " + runningContainers + " / " + containersRequested + " / " + containersToWaitFor);
        System.out.println(" -> Allocated containers " + containers.size());
        for (Container container : containers) {
            synchronized (this) {
                if (reducersSpawned.get() < NUM_REDUCERS.get()) {
                    reducersSpawned.incrementAndGet();
                    System.out.println(" -> Allocated container for reducer: " + container.getId().toString() + " on " + container.getNodeId().toString());
                    executeReducer(container);
                    continue;
                }
                allocatedContainers.add(container);
                System.out.println(" -> Allocated container for mapper: " + container.getId().toString() + " on " + container.getNodeId().toString());
                --containersRequested;
                executeMapper(container);
            }
        }
        System.out.println("After onContainersAllocated: running / requested / waiting for: " + runningContainers + " / " + containersRequested + " / " + containersToWaitFor);
    }

    private synchronized void estimateJobFinishTime() {
        int taskSum = 0;
        int parallelTasks = splits / PARALLEL_CONTAINERS.get();

        Iterator it = containerTimes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            TimeTuple t = (TimeTuple)pair.getValue();
            if (t.endTime != 0) {
                taskSum += (t.endTime - t.startTime);
            }
        }
        taskSum /= containerTimes.size();
        estimatedDeadline = taskSum * parallelTasks;

        System.out.println(" = After " + finishedContainers.size() + " Container(s): estimatedDeadline is " + estimatedDeadline + " ms = ");
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        //System.out.println("Before onContainersCompleted: running / requested / waiting for: " + runningContainers + " / " + containersRequested + " / " + containersToWaitFor);
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            synchronized (this) {
                --containersToWaitFor;
                --runningContainers;
                finishedContainers.add(status.getContainerId());
                containerTimes.get(status.getContainerId()).endTime = System.currentTimeMillis();
            }
            completedContainers.incrementAndGet();
        }
        estimateJobFinishTime();
        // Request new containers as the existing ones have completed
        // Make sure not more containers than PARALLEL_CONTAINERS are requested
        final int workingContainers = runningContainers + containersRequested;
        if (workingContainers < PARALLEL_CONTAINERS.get() && workingContainers < containersToWaitFor) {
            final int toRequest = PARALLEL_CONTAINERS.get() - Math.abs(runningContainers - containersRequested);
            if (toRequest > containersToWaitFor) {
                requestContainers(containersToWaitFor);
            } else {
                requestContainers(toRequest);
            }
        }
        System.out.println("Progress: " + getProgress());
        //System.out.println("After onContainersCompleted: running / requested / waiting for: " + runningContainers + " / " + containersRequested + " / " + containersToWaitFor);
    }

    private void clearRequestList() {
        synchronized (this) {
            for (ContainerRequest req : requestList) {
                rmClient.removeContainerRequest(req);
            }
            requestList.clear();
        }
    }

    private synchronized void requestContainers(int num) {
        System.out.println("Requesting " + num + " containers");
        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        clearRequestList();
        for (int i = 0; i < num; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            requestList.add(containerAsk);
            System.out.println("[AM] Making request " + absoluteContainersRequested);
            rmClient.addContainerRequest(containerAsk);
            ++absoluteContainersRequested;
            ++containersRequested;
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
        System.out.println("Nodes updated:");
        for (NodeReport report : updated) {
            System.out.println("Nodes: Rack = " +  report.getRackName() + ", Name = " + report.getNodeId().toString());
        }
    }

    public void onReboot() {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        final GetClusterNodesRequest nodes = GetClusterNodesRequest.newInstance();
        synchronized (this) {
            float progress = (float) completedContainers.get() / splits;

            if (appMasterConfig.increaseAfter1 >= 0.f) {
                if (increased1.get() == false && progress >= appMasterConfig.increaseAfter1) {
                    System.out.println("Increasing first time from " + PARALLEL_CONTAINERS.get() + " container(s) to " + appMasterConfig.increasedContainers1);
                    increased1.set(true);
                    PARALLEL_CONTAINERS.set(appMasterConfig.increasedContainers1);
                }
            }

            if (appMasterConfig.increaseAfter2 >= 0.f) {
                if (increased2.get() == false && progress >= appMasterConfig.increaseAfter2) {
                    System.out.println("Increasing second time from " + PARALLEL_CONTAINERS.get() + " container(s) to " + appMasterConfig.increasedContainers2);
                    increased2.set(true);
                    PARALLEL_CONTAINERS.set(appMasterConfig.increasedContainers2);
                }
            }

            return progress;
        }
    }

    public boolean doneWithContainers() {
        return containersToWaitFor == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
    /*
        List<Container> checkAllocatedContainers() {
            for (Container cnt : allocatedContainers) {
                try {
                    ContainerStatus status = nmClient.getContainerStatus(cnt.getId(), cnt.getNodeId());
                    if (status.getState() == ContainerState.COMPLETE) {
                    }
                } catch (YarnException | IOException e) {
                    throw new RuntimeException("Could not get status for container " + cnt.getId().toString() + ": " + e.getMessage());
                }

            }
        }
    */
    public void runMainLoop() throws Exception {
        rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();

        // Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");

        System.out.println("[AM] waiting for containers to finish");
        System.out.println("[AM] request container for reduce task");
        requestContainers(NUM_REDUCERS.get());
        requestContainers(PARALLEL_CONTAINERS.get());
        final TimeTuple absoluteTime = new TimeTuple(System.currentTimeMillis());
        long lastTime = 0;
        while (!doneWithContainers()) {
            //System.out.println("runMainLoop(): Running containers: " + runningContainers);
            //System.out.println("runMainLoop(): Waiting for containers: " + containersToWaitFor);
            System.out.println("Graph: " + (System.currentTimeMillis() - absoluteTime.startTime)/1000 + ";" + getProgress()*100.f);
            System.out.println("Finished Mappers: " + finishedMapper.size());
            Thread.sleep(1000);
        }
        absoluteTime.endTime = System.currentTimeMillis();
        System.out.println("Absolute job time: " + (absoluteTime.endTime - absoluteTime.startTime) + " ms");
        printContainerTimes();

        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }

    private void printContainerTimes() {
        Iterator it = containerTimes.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry pair = (Map.Entry)it.next();
            final ContainerId id = (ContainerId)pair.getKey();
            final TimeTuple t = (TimeTuple)pair.getValue();
            System.out.println(id.toString() + " = " + (t.endTime - t.startTime) + " ms, started at " + t.startTime + " ms");
            it.remove(); // avoids a ConcurrentModificationException
        }
    }

    public static void main(String[] args) throws Exception {
        final List<NodeId> nodes = new ArrayList<>();
        // Parse NodeIds given by Yarn Client
        for (int i = 2; i < args.length; i++) {
            final String[] nodeTuple = args[i].trim().split(":");
            nodes.add(NodeId.newInstance(nodeTuple[0], Integer.parseInt(nodeTuple[1])));
        }
        ApplicationMasterAsync master = new ApplicationMasterAsync(new Path(args[0]), new Path(args[1]), nodes);
        master.runMainLoop();
    }
}
