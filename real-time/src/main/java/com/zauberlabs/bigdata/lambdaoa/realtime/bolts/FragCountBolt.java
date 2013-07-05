/*
 * Copyright (c) 2013 MercadoLibre -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.bolts;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;

/**
 * Counter for fragger kills  
 * 
 * 
 * @since 10/05/2013
 */
public class FragCountBolt extends BaseRichBolt {
    
    /** <code>serialVersionUID</code> */
    private static final long serialVersionUID = 1828794992746417016L;
    
    private SortedMap<Long, Multiset<String>> fragsCounters = newMap(Maps.<Long, Multiset<String>>newHashMap());

    private OutputCollector collector;
    
    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals("drop_to_splout_source")) {
            writeToStoreFrom(tuple.getLongByField("time_frame"));
        } else {
            final String fragger = tuple.getString(tuple.fieldIndex("fragger"));
            
            final Long timeFrame = tuple.getLong(tuple.fieldIndex("time_frame"));
            
            Multiset<String> multiset = fragsCounters.get(timeFrame);
            
            if (multiset == null) {
                multiset = HashMultiset.create();
                fragsCounters.put(timeFrame, multiset);
            }
            multiset.add(fragger);
        }
        
        collector.ack(tuple);
    }
    
    public final void writeToStoreFrom(final Long ts) {
        this.fragsCounters = newMap(fragsCounters.tailMap(ts));
        
        System.out.println(fragsCounters);
    }
    
    private SortedMap<Long, Multiset<String>> newMap(Map<Long, Multiset<String>> n) {
        return new ConcurrentSkipListMap<Long, Multiset<String>>(n);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("time_frame", "fragger", "count"));
    }
}
