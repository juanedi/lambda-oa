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

import com.google.common.base.Objects;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.splout.db.common.SploutClient;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.DatePartitionedMap;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.FragStore;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.SploutUpdater;

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
    private DatePartitionedMap<Table<String, String, Integer>> fraggerFraggedCounter;
    private OutputCollector collector;
    private FragStore fragStore;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.fragStore = new SploutUpdater(new SploutClient(""));
        this.fraggerFraggedCounter = new DatePartitionedMap<Table<String, String, Integer>>(
            new Callable<Table<String, String, Integer>>() {
                @Override public Table<String, String, Integer> call() throws Exception {
                    return HashBasedTable.create();
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
            final Table<String, String, Integer> table = fraggerFraggedCounter.get(timeFrame);
            
            table.put(fragger, fragged, Objects.firstNonNull(table.get(fragger, fragged), 0) + 1);
        }
        
        collector.ack(tuple);
    }
    
    public synchronized final void writeToStoreFrom(final Date timeFrame) {
        this.fraggerFraggedCounter.dropLessThan(timeFrame);
        
        final Table<String, String, Integer> counters = HashBasedTable.create();
        
        for (final Table<String, String, Integer> multiset : fraggerFraggedCounter.getTarget().values()) {
            for (Cell<String, String, Integer> entry : multiset.cellSet()) {
                final String fragger = entry.getColumnKey();
                final String fragged = entry.getRowKey();
                final Integer base = Objects.firstNonNull(counters.get(fragger, fragged), 0);
                counters.put(fragger, fragged,  base + entry.getValue());
            }
        }
        
        fragStore.updateFragVersusCount(counters);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("fragger", "fragged", "count"));
    }

}