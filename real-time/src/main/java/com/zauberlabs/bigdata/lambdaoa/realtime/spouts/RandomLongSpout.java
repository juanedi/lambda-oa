package com.zauberlabs.bigdata.lambdaoa.realtime.spouts;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Spout that emits random records 
 * 
 * 
 * @since 05/07/2013
 */
public class RandomLongSpout extends BaseRichSpout {

    private static final long serialVersionUID = -2046124835159328252L;
    private SpoutOutputCollector collector;
    private Integer c = 0;

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        c++;
        if (c % 1000 == 0) {
            collector.emit(new Values(DateUtils.truncate(new Date(), Calendar.MINUTE).getTime()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time_frame"));
    }
 
    
    
}