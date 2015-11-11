package de.jth.ma.wc;

/**
 * Created by jth on 11/9/15.
 */
public final class AppMasterConfig {
    float   increaseAfter1;       // Increase after given % of threshold, <= 0.0 for no increase
    float   increaseAfter2;
    int     initialContainers;     // Initial containers
    int     increasedContainers1;  // Increase to that number of containers
    int     increasedContainers2;
    long    taskDuration;        // Task duration in ms

    @Override
    public String toString() {
        return "increaseAfter1: " + increaseAfter1 + ", increaseAfter2: " + increaseAfter2 + ", initialContainers: " + initialContainers
                + ", increasedContainers1: " + increasedContainers1 + ", increasedContainers2: " + increasedContainers2 + ", taskDuration: " + taskDuration;
    }
}
