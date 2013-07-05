/*
 * Copyright (c) 2013 MercadoLibre -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.bolts;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

import org.testng.collections.Maps;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.DatePartitionedMap;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.FragStore;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.NullFragStore;

/**
 * Counter for fragger kills  
 * 
 * 
 * @since 10/05/2013
 */
public class FragCountBolt extends BaseRichBolt {
    
    /** <code>serialVersionUID</code> */
    private static final long serialVersionUID = 1828794992746417016L;
    
    private DatePartitionedMap<Multiset<String>> fragsCounters;

    private OutputCollector collector;

    private FragStore fragStore;
    
    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
//        this.fragStore = new SploutUpdater(new SploutClient(""));
        this.fragStore = new NullFragStore();
        this.fragsCounters = new DatePartitionedMap<Multiset<String>>(new Callable<Multiset<String>>() {
                @Override public Multiset<String> call() throws Exception {
                    return HashMultiset.create();
                }
        });
    }

    @Override
    public void execute(Tuple tuple) {
        final Date timeFrame = new Date(tuple.getLong(tuple.fieldIndex("time_frame")));
        
        if(tuple.getSourceComponent().equals("drop_to_splout_source")) {
            writeToStoreFrom(timeFrame);
        } else {
            final String fragger = tuple.getString(tuple.fieldIndex("fragger"));
            
            fragsCounters.get(timeFrame).add(fragger);
        }
        
        collector.ack(tuple);
    }
    
    public final void writeToStoreFrom(final Date ts) {
        this.fragsCounters.dropLessThan(ts);
        
        final Map<String, Long> counters = Maps.newHashMap();
        for (final Multiset<String> multiset : fragsCounters.getTarget().values()) {
            for (String string : multiset.elementSet()) {
                final Long base = Objects.firstNonNull(counters.get(string), 0L);
                counters.put(string,  base + multiset.count(string));
            }
        }
        
        fragStore.updateFragCount(counters);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("time_frame", "fragger", "count"));
    }
}
