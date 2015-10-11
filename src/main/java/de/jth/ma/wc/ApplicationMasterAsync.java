package de.jth.ma.wc;

/**
 * Created by jth on 10/6/15.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {
    private final AtomicInteger PARALLEL_CONTAINERS = new AtomicInteger(1);
    private final AtomicBoolean increased = new AtomicBoolean(false);
    private static final long DEADLINE = 16000; // Deadline in ms for whole job completion
    private int estimatedDeadline = 0; // Estimated job finish
    private final Path inputPath;
    private final Path outputPath;
    private final int splits;
    private final List<ContainerRequest> requestList = new ArrayList<>();
    private final List<ContainerId> finishedContainers = new ArrayList<>();
    private final Map<ContainerId, TimeTuple> containerTimes = new HashMap<>();
    private List<Container> allocatedContainers = new ArrayList<>();
    private Configuration configuration;
    private NMClient nmClient;
    private AMRMClientAsync<ContainerRequest> rmClient;
    private InputSplitter splitter;
    // Counters, use atomic integer here?
    private final AtomicInteger completedContainers = new AtomicInteger(0);
    private int ctr = 0;
    private int absoluteContainersRequested = 0; // How many containers where requested overall during job?
    private int containersRequested = 0; // Containers requests pending
    private int containersToWaitFor;
    private int runningContainers = 0;

    private static class TimeTuple {
        public long startTime;
        public long endTime;

        TimeTuple(long startTime) {
            this.startTime = startTime;
            endTime = 0;
        }

        @Override
        public String toString() {
            return "startTime: " + startTime + ", endTime: " + endTime;
        }
    }

    public ApplicationMasterAsync(Path inputPath, Path outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        splitter = new InputSplitter(inputPath);
        try {
            splitter.stat();
        } catch (IOException e) {
            throw new RuntimeException("Could not split input: " + e.getMessage());
        }
        splits = containersToWaitFor = splitter.getStats().size();
        configuration = new YarnConfiguration();
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
    }

    private void execute(Container container) {
        final String command =
                "$JAVA_HOME/bin/java" +
                        " -Xmx128M" +
                        " de.jth.ma.wc.Mapper" +
                        " " + splitter.getStats().get(ctr).getPath().toString() +
                        " " + outputPath +
                        " " + ctr +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        try {
            // Launch container by create ContainerLaunchContext
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
            ctx.setCommands(Collections.singletonList(command));
            System.out.println("[AM] Launching container " + container.getId());
            synchronized (this) {
                containerTimes.put(container.getId(), new TimeTuple(System.currentTimeMillis()));
                ++runningContainers;
                ++ctr;
                nmClient.startContainer(container, ctx);
            }
            System.out.println("execute: running Containers: " + runningContainers);
        } catch (Exception ex) {
            System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
        }
    }

    public void onContainersAllocated(List<Container> containers) {
        System.out.println("Before onContainersAllocated: running / requested / waiting for: " + runningContainers + " / " + containersRequested + " / " + containersToWaitFor);
        System.out.println(" -> Allocated containers " + containers.size());
        for (Container container : containers) {
            synchronized (this) {
                allocatedContainers.add(container);
                System.out.println(" -> Allocated container: " + container.getId().toString());
                --containersRequested;
                execute(container);
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
    }

    public void onReboot() {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        synchronized (this) {
            float progress = (float) completedContainers.get() / splits;
            if (increased.get() == false && progress >= 0.5f) {
                System.out.println("Increasing from " + PARALLEL_CONTAINERS.get() + " container(s) to " + 4);
                increased.set(true);
                PARALLEL_CONTAINERS.set(4);
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
        requestContainers(PARALLEL_CONTAINERS.get());
        final TimeTuple absoluteTime = new TimeTuple(System.currentTimeMillis());
        while (!doneWithContainers()) {
            //System.out.println("runMainLoop(): Running containers: " + runningContainers);
            //System.out.println("runMainLoop(): Waiting for containers: " + containersToWaitFor);
            Thread.sleep(100);
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
        ApplicationMasterAsync master = new ApplicationMasterAsync(new Path(args[0]), new Path(args[1]));
        master.runMainLoop();
    }
}
