/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.util;

import java.util.Date;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang.UnhandledException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;

/**
 * TODO: Description of the class, Comments in english by default  
 * 
 * 
 * @since 05/07/2013
 */
public class  DatePartitionedMap<T> {
    
    private final Callable<T> defaulter;
    private SortedMap<Date, T> target;
    
    /** Creates the DatePartitionedMap */
    public DatePartitionedMap(final Callable<T> d) {
        this.defaulter = Preconditions.checkNotNull(d);
        this.target = new ConcurrentSkipListMap<Date, T>();
    }
    
    public final void dropLessThan(final Date ts) {
        this.target = new ConcurrentSkipListMap<Date, T>(target.tailMap(ts));
    }
    
    public final T get(final Date k) {
        T t = target.get(k);
        if (t == null) {
            try {
                t = defaulter.call();
                target.put(k, t);
            } catch (Exception e) {
                throw new UnhandledException(e);
            }
        }
        return t;
    }
    
    /** Returns the target */
    public final SortedMap<Date, T> getTarget() {
        return ImmutableSortedMap.copyOf(target);
    }
    
}
