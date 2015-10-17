package de.jth.ma.wc;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.dag.app.rm.TezAMRMClientAsync;
import org.apache.tez.serviceplugins.api.ContainerLauncher;

import java.io.IOException;
import java.util.List;

/**
 * Created by jth on 10/17/15.
 */
public class ApplicationMasterTez implements TezAMRMClientAsync.CallbackHandler {
    private TezAMRMClientAsync<AMRMClient.ContainerRequest> rmClient;
    private ContainerLauncher containerLauncher;
    private final YarnConfiguration configuration;

    ApplicationMasterTez() {
        System.out.println("Registering TezApplicationMaster");
        configuration = new YarnConfiguration();
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

    @Override
    public void onContainersCompleted(List<ContainerStatus> list) {

    }

    @Override
    public void onContainersAllocated(List<Container> list) {

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
        ApplicationMasterTez master = new ApplicationMasterTez();
        master.runMainLoop();
    }
}
