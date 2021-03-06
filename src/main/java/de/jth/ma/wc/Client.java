package de.jth.ma.wc;

/**
 * This class is the YARN client (not the app master), which launches the ApplicationMaster
 * for the WordCount case manually.
 *
 * Created by jth
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Client {

    Configuration conf = new YarnConfiguration();

    public void run(String[] args) {
        final String inputPath = args[0];
        final String outputPath = args[1];
        final Path jarPath = new Path(args[2]);

        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Create application via yarnClient
        YarnClientApplication app = null;
        try {
            app = yarnClient.createApplication();
        } catch (YarnException | IOException e) {
            System.out.println("yarnClient.createApplication() failed: " + e.getMessage());
            System.exit(0);
        }

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer =
                Records.newRecord(ContainerLaunchContext.class);

        final String cmd =
                "$JAVA_HOME/bin/java" +
                        " -Xmx128M" +
                        " de.jth.ma.wc.ApplicationMasterTez" +
                        " " + inputPath +
                        " " + outputPath +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        System.out.println("Running command: " + cmd);

        amContainer.setCommands(Collections.singletonList(cmd));

        // Setup jar for ApplicationMaster
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        try {
            setupAppMasterJar(jarPath, appMasterJar);
        } catch (IOException e) {
            System.out.println("Could not setup AppMasterJar: " + e.getMessage());
            System.exit(0);
        }
        // Necessary?
        amContainer.setLocalResources(Collections.singletonMap("wordcount.jar", appMasterJar));

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        // The AppMaster itself doesn't need much
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext =
                app.getApplicationSubmissionContext();
        appContext.setApplicationName("jth-wc"); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        try {
            yarnClient.submitApplication(appContext);
        } catch (YarnException | IOException e) {
            System.out.println("Could not submit application: " + e.getMessage());
            System.exit(0);
        }

        ApplicationReport appReport = null;
        try {
            appReport = yarnClient.getApplicationReport(appId);
        } catch (YarnException | IOException e) {
            System.out.println("Could not get Application report: " + e.getMessage());
            System.exit(0);
        }
        YarnApplicationState appState = appReport.getYarnApplicationState();
        // Do busy waiting until the AppMaster finishes
        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED) {
            try {
                Thread.sleep(100);
                appReport = yarnClient.getApplicationReport(appId);
            } catch (YarnException | IOException | InterruptedException e) {
                System.out.println("Could not get Application Report: " + e.getMessage());
                System.exit(0);
            }
            appState = appReport.getYarnApplicationState();
        }

        System.out.println(
                "Application " + appId + " finished with" +
                        " state " + appState +
                        " at " + appReport.getFinishTime());

    }

    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        System.out.println("jarStat: " + jarStat.toString());
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        Apps.addToEnvironment(appMasterEnv,
                Environment.CLASSPATH.name(),
                "/home/jth/work/MA/build/tez/*");
        Apps.addToEnvironment(appMasterEnv,
                Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            System.out.println("Adding to classpath: " + c.trim());
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                    c.trim());
        }
    }

    public static void main(String[] args) {
        Client c = new Client();
        c.run(args);
    }
}
