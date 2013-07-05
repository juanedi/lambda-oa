/*
 * Copyright (c) 2013 MercadoLibre -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.bolts;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.zauberlabs.bigdata.lambdaoa.realtime.util.DatePartitionedMap;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.FragStore;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.NullFragStore;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.VsCount;

/**
 *   Mantains a fragger fragged count matrix
 * 
 * 
 * @author Joel Cueto
 * @since 10/05/2013
 */
@SuppressWarnings("rawtypes")
public class FraggerFraggedCountBolt extends BaseRichBolt {
    
    /** <code>serialVersionUID</code> */
    private static final long serialVersionUID = -2261986721084451354L;
    private DatePartitionedMap<VsCount> fraggerFraggedCounter;
    private OutputCollector collector;
    private FragStore fragStore;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
//        this.fragStore = new SploutUpdater(new SploutClient(""));
        this.fragStore = new NullFragStore();
        this.fraggerFraggedCounter = new DatePartitionedMap<VsCount>(new Callable<VsCount>() {
                @Override public VsCount call() throws Exception {
                    return new VsCount();
                }
        });
    }

    @Override
    public synchronized void execute(Tuple tuple) {
        final Date timeFrame = new Date(tuple.getLongByField("time_frame"));
        if(tuple.getSourceComponent().equals("drop_to_splout_source")) {
            writeToStoreFrom(timeFrame);
        } else {
            final String fragger = tuple.getStringByField("fragger");
            final String fragged = tuple.getStringByField("fragged");
            
            fraggerFraggedCounter.get(timeFrame).update(fragger, fragged);
        }
        
        collector.ack(tuple);
    }
    
    public synchronized final void writeToStoreFrom(final Date timeFrame) {
        this.fraggerFraggedCounter.dropLessThan(timeFrame);
        
        VsCount sum = new VsCount();
        for (final VsCount multiset : fraggerFraggedCounter.getTarget().values()) {
            sum = sum.sum(multiset);
        }
        
        fragStore.updateFragVersusCount(sum);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("fragger", "fragged", "count"));
    }

}