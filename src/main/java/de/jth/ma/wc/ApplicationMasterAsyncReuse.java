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
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMasterAsyncReuse implements AMRMClientAsync.CallbackHandler {
    private static final boolean REUSE_CONTAINERS = true;
    private static final long DEADLINE = 16000; // Deadline in ms for whole job completion

    private final AtomicInteger PARALLEL_CONTAINERS = new AtomicInteger(1);
    private final AtomicBoolean increased = new AtomicBoolean(false);
    private int estimatedDeadline = 0; // Estimated job finish
    private final Path outputPath;
    private Configuration configuration;
    private NMClient nmClient;
    private AMRMClientAsync<ContainerRequest> rmClient;
    private InputSplitter splitter;
    private final AtomicInteger complatedTasks = new AtomicInteger(0);
    private int absoluteContainersRequested = 0; // How many containers where requested overall during job?
    private int pendingRequests = 0; // Containers requests pending
    private int tasksToWaitFor;
    private int runningContainers = 0;

    private final List<MapTask> tasks = new ArrayList<>();
    private final List<ContainerRequest> requestList = new ArrayList<>();
    private Map <Container, Boolean> allocatedContainers = new HashMap<>();

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

    private void initializeTasks(int splits) {
        for (int i = 0; i < splits; ++i) {
            tasks.add(new MapTask(i));
        }
    }

    public ApplicationMasterAsyncReuse(Path inputPath, Path outputPath) {
        int splits;

        this.outputPath = outputPath;
        splitter = new InputSplitter(inputPath);
        try {
            splitter.stat();
        } catch (IOException e) {
            throw new RuntimeException("Could not split input: " + e.getMessage());
        }
        splits = tasksToWaitFor = splitter.getStats().size();
        initializeTasks(splits);
        configuration = new YarnConfiguration();
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
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

        try {
            final ContainerStatus status = nmClient.getContainerStatus(container.getId(), container.getNodeId());
            System.out.println("Status of container " + container.getId().toString() + ": " + status.getState().toString());
            Thread.sleep(100);
        } catch (YarnException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
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
                allocatedContainers.put(container, true);
                ++runningContainers;
                nmClient.startContainer(container, task.launchContext);
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

    public void onContainersAllocated(List<Container> containers) {
        System.out.println("Before onContainersAllocated: running / requested / waiting for: " + runningContainers + " / " + pendingRequests + " / " + tasksToWaitFor);
        System.out.println(" -> Allocated containers " + containers.size());
        for (Container container : containers) {
            synchronized (this) {
                allocatedContainers.put(container, false);
                System.out.println(" -> Allocated container: " + container.getId().toString());
                --pendingRequests;
                execute(container);
            }
        }
        System.out.println("After onContainersAllocated: running / requested / waiting for: " + runningContainers + " / " + pendingRequests + " / " + tasksToWaitFor);
    }

    private synchronized void estimateJobFinishTime() {
        /*
        int taskSum = 0;
        int parallelTasks = tasks.size() / PARALLEL_CONTAINERS.get();

        for (MapTask task : tasks) {

        }
        Iterator it = taskTimes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            TimeTuple t = (TimeTuple)pair.getValue();
            if (t.endTime != 0) {
                taskSum += (t.endTime - t.startTime);
            }
        }
        taskSum /= taskTimes.size();
        estimatedDeadline = taskSum * parallelTasks;

        System.out.println(" = After " + tasks.size() + " Tasks(s): estimatedDeadline is " + estimatedDeadline + " ms = ");
        */
    }

    private void markContainerAsIdle(ContainerId containerId) {
        System.out.println("Marking container " + containerId.toString() + " as idle");
        for (Container container : allocatedContainers.keySet()) {
            if (containerId.getContainerId() == container.getId().getContainerId()) {
                allocatedContainers.put(container, false);
                return;
            }
        }
    }

    private MapTask getCompletedTask(ContainerId containerId) {
        for (MapTask task : tasks) {
            if (task.mappedContainer == null) {
                continue;
            }
            if (task.mappedContainer.getId().getContainerId() == containerId.getContainerId()) {
                return task;
            }
        }
        return null;
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        //System.out.println("Before onContainersCompleted: running / requested / waiting for: " + runningContainers + " / " + pendingRequests + " / " + tasksToWaitFor);
        for (ContainerStatus status : statuses) {
            final MapTask task = getCompletedTask(status.getContainerId());
            synchronized (this) {
                System.out.println("[AM] Completed container task " + task.id + " on container " + status.getContainerId());
                try {
                    nmClient.stopContainer(task.mappedContainer.getId(), task.mappedContainer.getNodeId());
                    rmClient.releaseAssignedContainer(task.mappedContainer.getId());
                } catch (YarnException | IOException e) {
                    System.out.println("Could not stop container: " + e.getMessage());
                    e.printStackTrace();
                }
                System.out.println("Stopped container " + task.mappedContainer.getId());
                task.finished = true;
                task.running = false;
                --tasksToWaitFor;
                --runningContainers;
                task.endTime = System.currentTimeMillis();
                markContainerAsIdle(status.getContainerId());
            }
            complatedTasks.incrementAndGet();
            // Execute new task on existing container
            execute(task.mappedContainer);
        }
        estimateJobFinishTime();
        // Request new containers as the existing ones have completed
        // Make sure not more containers than PARALLEL_CONTAINERS are requested
        final int workingContainers = runningContainers + pendingRequests;
        if (workingContainers < PARALLEL_CONTAINERS.get() && workingContainers < tasksToWaitFor) {
            final int toRequest = PARALLEL_CONTAINERS.get() - Math.abs(runningContainers - pendingRequests);
            if (toRequest > tasksToWaitFor) {
                requestContainers(tasksToWaitFor);
            } else {
                requestContainers(toRequest);
            }
        }
        System.out.println("Progress: " + getProgress());
        //System.out.println("After onContainersCompleted: running / requested / waiting for: " + runningContainers + " / " + pendingRequests + " / " + tasksToWaitFor);
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
            ++pendingRequests;
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
            float progress = (float) complatedTasks.get() / tasks.size();
            if (increased.get() == false && progress >= 0.5f) {
                System.out.println("Increasing from " + PARALLEL_CONTAINERS.get() + " container(s) to 4");
                increased.set(true);
                PARALLEL_CONTAINERS.set(4);
            }
            return progress;
        }
    }

    public boolean doneWithContainers() {
        return tasksToWaitFor == 0;
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
            Thread.sleep(100);
        }
        absoluteTime.endTime = System.currentTimeMillis();
        System.out.println("Absolute job time: " + (absoluteTime.endTime - absoluteTime.startTime) + " ms");
        printTaskTimes();

        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }

    private void printTaskTimes() {
        for (MapTask t : tasks) {
            System.out.println("Task " + t.id + " = " + (t.endTime - t.startTime) + " ms, started at " + t.startTime + " ms, ended at " + t.endTime);
        }
    }

    public static void main(String[] args) throws Exception {
        ApplicationMasterAsyncReuse master = new ApplicationMasterAsyncReuse(new Path(args[0]), new Path(args[1]));
        master.runMainLoop();
    }
}
