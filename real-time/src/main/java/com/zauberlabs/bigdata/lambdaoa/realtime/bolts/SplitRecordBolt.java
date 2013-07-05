/*
 * Copyright (c) 2013 MercadoLibre -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.bolts;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

/**
 * Splits a CSV record into fields  
 * 
 * 
 * @author Joel Cueto
 * @since 10/05/2013
 */
@SuppressWarnings("rawtypes")
public class SplitRecordBolt extends BaseRichBolt {
    
    private static final long serialVersionUID = 4209433581032330235L;
    
    private OutputCollector collector;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time_frame", "game", "fragger", "fragged"));
    }

    @Override public void execute(Tuple input) {
        List<Object> split = Lists.newArrayList();
        split.add(input.getLongByField("time_frame"));
        split.addAll(Lists.newArrayList(input.getStringByField("record").split(",")));
        collector.emit(input, new Values(split.toArray()));
        
        collector.ack(input);
    }
}  