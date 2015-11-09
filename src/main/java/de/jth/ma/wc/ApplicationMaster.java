package de.jth.ma.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

public class ApplicationMaster {
    private int splits;
    // For testing purposes, how many containers to run in parallel initially.
    private int parallel_containers = 1;
    private int containersRequested = 0;
    private int containersGranted   = 0;
    private AMRMClient<ContainerRequest> rmClient;
    private NMClient nmClient;
    private Path inputPath;
    private Path outputPath;
    private InputSplitter splitter;
    private Configuration configuration;
    private final Map<ContainerId, TimeTuple> times = new HashMap<>();
    private final List<ContainerId> runningContainers = new ArrayList<>();
    private final List<ContainerId> finishedContainers = new ArrayList<>();
    private final List<ContainerRequest> requests = new ArrayList<>();

    private static class TimeTuple {
        public long startTime;
        public long endTime;

        TimeTuple() {
            startTime = 0;
            endTime = 0;
        }
    }

    public void register() throws IOException, YarnException {
        splitter = new InputSplitter(inputPath);
        splitter.stat();
        splits = splitter.getStats().size();
        System.out.println("Number of input splits: " + splits);

        // Initialize clients to ResourceManager and NodeManagers
        configuration = new YarnConfiguration();

        rmClient = AMRMClient.createAMRMClient();
        rmClient.init(configuration);
        rmClient.start();

        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();

        // Register with ResourceManager
        System.out.println("registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("registerApplicationMaster 1");
    }

    private void askForContainers(int num) {
        System.out.println("Asking for " + num + " containers");
        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Make container requests to ResourceManager
        for (int i = 0; i < num; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            //System.out.println("Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
            requests.add(containerAsk);
        }
        containersRequested += num;
    }

    private void printRuntimes() {
        Iterator it = times.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            ContainerId id = (ContainerId)pair.getKey();
            TimeTuple t = (TimeTuple)pair.getValue();
            System.out.println(id.toString() + " = " + (t.endTime - t.startTime) + " ms");
            it.remove(); // avoids a ConcurrentModificationException
        }
    }

    private List<String> getAllNodes() {
        System.out.println("Nodes in the cluster: " + rmClient.getClusterNodeCount());
        return null;
    }

    private void removeGrantedRequests() {
    }

    public void run() throws IOException, YarnException, InterruptedException {
        // Obtain allocated containers, launch and check for responses
        int responseId = 0;

        askForContainers(parallel_containers);

        while (finishedContainers.size() < splits) {
            AllocateResponse response = rmClient.allocate(responseId++);

            // .getAllocatedContainers gives the absolute number of granted containers
            for (Container container : response.getAllocatedContainers()) {
                System.out.println("RM granted " + response.getAllocatedContainers().size() + " containers");
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);
                final String cmd =
                        "$JAVA_HOME/bin/java" +
                                " -Xmx128M" +
                                " de.jth.ma.wc.Mapper" +
                                " " + splitter.getStats().get(containersGranted).getPath().toString() +
                                " " + outputPath +
                                " " + containersGranted +
                                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
                ctx.setCommands(Collections.singletonList(cmd));

                System.out.println("Launching container " + container.getId());
                TimeTuple t = new TimeTuple();
                t.startTime = System.currentTimeMillis();
                times.put(container.getId(), t);
                nmClient.startContainer(container, ctx);
                runningContainers.add(container.getId());
                // Remove old resource requests
                for (ContainerRequest r : requests) {
                    rmClient.removeContainerRequest(r);
                }
                requests.clear();
                containersGranted++;
            }

            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                if (times.get(status.getContainerId()).endTime == 0) {
                    times.get(status.getContainerId()).endTime = System.currentTimeMillis();
                }
                System.out.println("Completed container " + status.getContainerId());
                runningContainers.remove(status.getContainerId());
                finishedContainers.add(status.getContainerId());
                //askForContainers(parallel_containers - runningContainers.size());
                // Dödö, this can reassign freed containers, so the matching isn't correct anymore
                //rmClient.releaseAssignedContainer(status.getContainerId());

                if (runningContainers.size() < parallel_containers && requests.isEmpty() && finishedContainers.size() < splits) {
                    System.out.println("Finished Containers: " + finishedContainers.size());
                    askForContainers(parallel_containers - runningContainers.size());
                }
            }

            System.out.println("Containers requested / granted: " + containersRequested + " / " + containersGranted);
            System.out.println("Running containers: " + runningContainers.size());
            System.out.println("Finished containers: " + finishedContainers.size());

            // Isn't optimal, tampers a little bit with the measuring times
            Thread.sleep(50);
        }

        printRuntimes();

        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");

    }

    ApplicationMaster(Path inputPath, Path outputPath) throws IOException, YarnException, InterruptedException {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        System.out.println("Entered ApplicationMaster");
        System.out.println("Input-Path: " + inputPath.toString());
        System.out.println("Output-Path: " + outputPath.toString());
    }

    public static void main(String[] args) throws Exception {
        TimeTuple absoluteTime = new TimeTuple(); // job completion time
        ApplicationMaster am = new ApplicationMaster(new Path(args[0]), new Path(args[1]));
        absoluteTime.startTime = System.currentTimeMillis();
        am.register();
        am.run();
        absoluteTime.endTime = System.currentTimeMillis();
        System.out.println("Job completion time: " + (absoluteTime.endTime - absoluteTime.startTime));
    }
}
