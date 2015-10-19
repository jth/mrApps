package de.jth.ma.wc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.dag.app.rm.TezAMRMClientAsync;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jth on 10/17/15.
 */
public class ApplicationMasterTez implements TezAMRMClientAsync.CallbackHandler {
    private TezAMRMClientAsync<AMRMClient.ContainerRequest> rmClient;
    private final Path outputPath;
    private final InputSplitter splitter;
    private final AtomicInteger absoluteContainersRequested = new AtomicInteger(0);
    private final AtomicInteger pendingRequests = new AtomicInteger(0);
    private final AtomicInteger runningContainers = new AtomicInteger(0);
    private final AtomicInteger tasksToWaitFor;
    private final YarnConfiguration configuration;
    private final Queue<AMRMClient.ContainerRequest> requestQueue = new ConcurrentLinkedQueue<>();
    private final Queue<MapTask> tasks = new ConcurrentLinkedQueue<>();
    private final Queue<ContainerLauncher> launchers = new ConcurrentLinkedQueue<>();
    private final Map<Container, Boolean> allocatedMap = new ConcurrentHashMap<>();

    ApplicationMasterTez(Path inputPath, Path outputPath) {
        int splits;

        System.out.println("Registering TezApplicationMaster");
        configuration = new YarnConfiguration();
        this.outputPath = outputPath;
        splitter = new InputSplitter(inputPath);
        try {
            splitter.stat();
        } catch (IOException e) {
            throw new RuntimeException("Could not split input: " + e.getMessage());
        }
        splits = splitter.getStats().size();
        tasksToWaitFor = new AtomicInteger(splits);
        initializeTasks(splits);
    }

    private void initializeTasks(int splits) {
        for (int i = 0; i < splits; ++i) {
            tasks.add(new MapTask(i));
        }
    }

    private void runMainLoop() throws IOException, YarnException, InterruptedException {
        rmClient = TezAMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(configuration);
        rmClient.start();

        System.out.println("[AM] registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");
        Thread.sleep(5000);
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregistered application master");
    }

    private void clearRequestList() {
        synchronized (this) {
            for (AMRMClient.ContainerRequest req : requestQueue) {
                rmClient.removeContainerRequest(req);
            }
            requestQueue.clear();
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
            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
            requestQueue.add(containerAsk);
            System.out.println("[AM] Making request " + absoluteContainersRequested);
            rmClient.addContainerRequest(containerAsk);
            absoluteContainersRequested.incrementAndGet();
            pendingRequests.incrementAndGet();
        }
    }

    private MapTask getNextWaitingTask() {
        for (MapTask task : tasks) {
            if (!task.finished && !task.running) {
                return task;
            }
        }
        return null;
    }

    private void execute(Container container) {
        final MapTask task = getNextWaitingTask();

        if (task == null) {
            System.out.println("execute(): No tasks left, doing nothing");
            return;
        }
/*
        try {
            final ContainerStatus status = nmClient.getContainerStatus(container.getId(), container.getNodeId());
            System.out.println("Status of container " + container.getId().toString() + ": " + status.getState().toString());
            Thread.sleep(100);
        } catch (YarnException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
        */
        final String command =
                "$JAVA_HOME/bin/java" +
                        " -Xmx128M" +
                        " de.jth.ma.wc.Mapper" +
                        " " + splitter.getStats().get(task.id).getPath().toString() +
                        " " + outputPath +
                        " " + task.id +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout_" + task.id +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr_" + task.id;
        try {

            // Launch container by create ContainerLaunchContext
            //if (task.launchContext == null) {
            // Todo: Try this...
            //ContainerLaunchContext.newInstance()
            // Create container launch context in a proper way...
            task.launchContext = Records.newRecord(ContainerLaunchContext.class);
            //}
            task.launchContext.setCommands(Collections.singletonList(command));
            System.out.println("[AM] Launching task " + task.id + " on container " + container.getId());
            synchronized (this) {
                task.running = true;
                task.startTime = System.currentTimeMillis();
                task.mappedContainer = container;
                allocatedMap.put(container, true);
                runningContainers.incrementAndGet();
                launchers.add(new ContainerLauncher(task.launchContext) {
                    @Override
                    public void launchContainer(ContainerLaunchRequest containerLaunchRequest) {

                    }

                    @Override
                    public void stopContainer(ContainerStopRequest containerStopRequest) {

                    }
                })
            }
            System.out.println("execute: running Containers: " + runningContainers);
        } catch (Exception ex) {
            System.err.println("[AM] Error launching container " + container.getId() + " " + ex + " " + ex.getMessage());
            ex.printStackTrace();
            try {
                rmClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, "", "");
            } catch (YarnException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> list) {

    }

    @Override
    public void onContainersAllocated(List<Container> list) {
        System.out.println("Before onContainersAllocated: running / requested / waiting for: "
                + runningContainers.get() + " / " + pendingRequests.get() + " / " + tasksToWaitFor.get());
        System.out.println(" -> Allocated containers " + list.size());
        for (Container container : list) {
            synchronized (this) {
                allocatedMap.put(container, false);
                System.out.println(" -> Allocated container: " + container.getId().toString());
                pendingRequests.decrementAndGet();
                execute(container);
            }
        }
        System.out.println("After onContainersAllocated: running / requested / waiting for: "
                + runningContainers.get() + " / " + pendingRequests.get() + " / " + tasksToWaitFor.get());

    }

    @Override
    public void onShutdownRequest() {

    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {

    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable throwable) {

    }

    public static void main(String[] args) throws Exception {
        System.out.println("Entering ApplicationMasterTez");
        ApplicationMasterTez master = new ApplicationMasterTez(new Path(args[0]), new Path(args[1]));
        master.runMainLoop();
    }
}
