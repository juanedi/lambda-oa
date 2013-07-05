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

import com.google.common.base.Objects;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

/**
 *   Mantains a fragger fragged count matrix
 * 
 * 
 * @author Joel Cueto
 * @since 10/05/2013
 */
public class FraggerFraggedCountBolt extends BaseRichBolt {
    
    /** <code>serialVersionUID</code> */
    private static final long serialVersionUID = -2261986721084451354L;
    private SortedMap<Long,  Table<String, String, Integer>> fraggerFraggedCounter;
    private OutputCollector collector;
    
    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.fraggerFraggedCounter = newMap(Maps.<Long, Table<String, String, Integer>>newTreeMap());
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals("drop_to_splout_source")) {
            writeToStoreFrom(tuple.getLongByField("time_frame"));
        } else {
            final String fragger = tuple.getStringByField("fragger");
            final String fragged = tuple.getStringByField("fragged");
            final Long timeFrame = tuple.getLongByField("time_frame");
            Table<String, String, Integer> table = fraggerFraggedCounter.get(timeFrame);
            if (table == null) {
                table = HashBasedTable.create();
                fraggerFraggedCounter.put(timeFrame, table);
            }
            
            table.put(fragger, fragged, Objects.firstNonNull(table.get(fragger, fragged), 0) + 1);
        }
        
        collector.ack(tuple);
    }
    
    public final void writeToStoreFrom(final Long ts) {
        this.fraggerFraggedCounter = newMap(fraggerFraggedCounter.tailMap(ts));
        
        System.out.println(fraggerFraggedCounter);
    }

    private SortedMap<Long, Table<String, String, Integer>> newMap(SortedMap<Long,  Table<String, String, Integer>> n) {
        
        return new ConcurrentSkipListMap<Long, Table<String, String, Integer>>(n);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("fragger", "fragged", "count"));
    }

}