/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.bolts;

import static junit.framework.Assert.*;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Before;
import org.junit.Test;
import org.testng.collections.Lists;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;

import com.zauberlabs.bigdata.lambdaoa.realtime.fragstore.FragStore;
import com.zauberlabs.bigdata.lambdaoa.realtime.util.VsCount;

/**
 * {@link FragCountBolt} test  
 * 
 * 
 * @since 05/07/2013
 */
public class FragCountBoltTest {
    
    private FragCountBolt bolt;
    private IOutputCollector collector;

    @Before
    public final void setup() {
        this.bolt = new FragCountBolt();
        this.bolt.prepare(null, null, new OutputCollector(collector));
    }
    
    @Test
    public final void should_increment_counters() {
        bolt.incrementFraggerForTime(new Date(), "a");
        bolt.incrementFraggerForTime(new Date(), "a");
        bolt.incrementFraggerForTime(DateUtils.addDays(new Date(), -2), "b");
        
        final List<Map<String, Long>> counts = Lists.newArrayList();
        this.bolt.setFragStore(new FragStore() {
            @Override public void updateFragVersusCount(VsCount fraggers) {
                
            }
            
            @Override public void updateFragCount(Map<String, Long> fraggers) {
                counts.add(fraggers);                
            }
        });
        bolt.writeToStoreFrom(DateUtils.addDays(new Date(), -1));
        
        assertTrue(1 == counts.size());
        assertTrue(2 == counts.get(0).get("a"));
    }

    @Test
    public final void should_not_explode_on_empty_flush_to_store() {
        
        final List<Map<String, Long>> counts = Lists.newArrayList();
        
        this.bolt.setFragStore(new FragStore() {
            @Override public void updateFragVersusCount(VsCount fraggers) {
                
            }
            
            @Override public void updateFragCount(Map<String, Long> fraggers) {
                counts.add(fraggers);                
            }
        });
        bolt.writeToStoreFrom(DateUtils.addDays(new Date(), -1));

        assertTrue(1 == counts.size());
        assertTrue(counts.get(0).isEmpty());
    }
}
