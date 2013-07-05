package com.zauberlabs.bigdata.lambdaoa.realtime.util;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

/**
 * Runs storm topology 
 * 
 * @since 05/07/2013
 */
public final class StormRunner {

    private static final int MILLIS_IN_SEC = 1000;

    private StormRunner() {
        
    }

    public static void runTopologyLocally(
        final StormTopology topology, final String topologyName,
        final Config conf, final int runtimeInSeconds) throws InterruptedException {
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }
}
