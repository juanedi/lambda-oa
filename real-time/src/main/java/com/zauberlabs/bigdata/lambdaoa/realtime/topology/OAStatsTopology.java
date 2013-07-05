package com.zauberlabs.bigdata.lambdaoa.realtime.topology;

import com.zauberlabs.bigdata.lambdaoa.realtime.bolts.FragCountBolt;
import com.zauberlabs.bigdata.lambdaoa.realtime.bolts.FraggerFraggedCountBolt;
import com.zauberlabs.bigdata.lambdaoa.realtime.bolts.SplitRecordBolt;
import com.zauberlabs.bigdata.lambdaoa.realtime.spouts.RabbitmqSpout;
import com.zauberlabs.bigdata.lambdaoa.realtime.spouts.RandomLongSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public final class OAStatsTopology {
    
    /** Creates the OAStatsTopology **/
    private OAStatsTopology() {
        
    }
    
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("record_source", new RabbitmqSpout(), 1);
        
        builder.setSpout("drop_to_splout_source", new RandomLongSpout(), 1);

        builder.setBolt("record_split", new SplitRecordBolt(), 8).shuffleGrouping("record_source");

        builder.setBolt("frag_count", new FragCountBolt(), 12)
            .fieldsGrouping("record_split", new Fields("fragger", "time_frame"))
            .shuffleGrouping("drop_to_splout_source");

        builder.setBolt("frag_matrix_count", new FraggerFraggedCountBolt(), 12)
            .fieldsGrouping("record_split", new Fields("fragger", "time_frame"))
            .shuffleGrouping("drop_to_splout_source");
        
        Config conf = new Config();

//        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(60 * 60 * 1000);
//            Thread.sleep(10000);

//            cluster.shutdown();
        }
    }
}
