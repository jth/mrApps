package de.jth.ma.wc;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;

/**
 * Created by jth on 10/15/15.
 */
public class MapTask {
    public final int id; // task id, correpsonds to one input split
    public boolean finished = false; // tasks finished
    public boolean running = false; // tasks is running atm
    public Container mappedContainer = null; // container the task is mapped to
    public ContainerLauncherContext context = null;
    public long startTime = 0;
    public long endTime = 0;

    MapTask(int id) {
        this.id = id;
    }

}
