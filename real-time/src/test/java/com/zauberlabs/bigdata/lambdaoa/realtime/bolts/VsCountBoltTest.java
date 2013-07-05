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
 * {@link VsCountBolt} test  
 * 
 * 
 * @since 05/07/2013
 */
public class VsCountBoltTest {
    private VsCountBolt bolt;
    private IOutputCollector collector;

    @Before
    public final void setup() {
        this.bolt = new VsCountBolt();
        this.bolt.prepare(null, null, new OutputCollector(collector));
    }
    
    @Test
    public final void should_increment_counters() {
        bolt.incrementForDate(new Date(), "a", "b");
        bolt.incrementForDate(new Date(), "a", "b");
        bolt.incrementForDate(DateUtils.addDays(new Date(), -2), "b", "c");
        
        final List<VsCount> counts = Lists.newArrayList();
        this.bolt.setFragStore(new FragStore() {
            @Override public void updateFragVersusCount(VsCount fraggers) {
                counts.add(fraggers);                
            }
            @Override public void updateFragCount(Map<String, Long> fraggers) { }
        });
        bolt.writeToStoreFrom(DateUtils.addDays(new Date(), -1));
        
        assertTrue(1 == counts.size());
        assertTrue(1 == counts.get(0).getTarget().size());
        assertTrue(2 == counts.get(0).getTarget().get("a", "b"));
    }

    @Test
    public final void should_not_explode_on_empty_flush_to_store() {
        
        final List<VsCount> counts = Lists.newArrayList();
        
        this.bolt.setFragStore(new FragStore() {
            @Override public void updateFragVersusCount(VsCount fraggers) {
                counts.add(fraggers);                
            }
            @Override public void updateFragCount(Map<String, Long> fraggers) { }
        });
        bolt.writeToStoreFrom(DateUtils.addDays(new Date(), -1));

        assertTrue(1 == counts.size());
        assertTrue(counts.get(0).getTarget().isEmpty());
    }

}
