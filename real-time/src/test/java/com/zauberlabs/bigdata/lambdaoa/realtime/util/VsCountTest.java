/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.util;

import static junit.framework.Assert.*;

import org.junit.Test;

import com.google.common.collect.ImmutableTable;

/**
 * TODO: Description of the class, Comments in english by default  
 * 
 * 
 * @since 05/07/2013
 */
public class VsCountTest {

    @Test
    public final void should_merge_empty_counts_as_empty() {
        VsCount vsCount = new VsCount();
        VsCount vsCount2 = new VsCount();
        assertTrue(vsCount.sum(vsCount2).getTarget().isEmpty());
    }
    
    @Test
    public final void should_merge_empty_and_non_empty() {
        VsCount vsCount = new VsCount();
        VsCount vsCount2 = new VsCount(ImmutableTable.of("a", "b", 2));
        assertEquals(vsCount.sum(vsCount2).getTarget(), ImmutableTable.of("a", "b", 2));
    }

    
    @Test
    public final void should_merge_non_empty_counts() {
        VsCount vsCount = new VsCount(ImmutableTable.of("a", "b", 4));
        VsCount vsCount2 = new VsCount(ImmutableTable.of("a", "b", 2));
        assertEquals(vsCount.sum(vsCount2).getTarget(), ImmutableTable.of("a", "b", 6));
    }
}
