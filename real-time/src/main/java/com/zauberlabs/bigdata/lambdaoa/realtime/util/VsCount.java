/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.util;

import com.google.common.base.Objects;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

/**
 * TODO: Description of the class, Comments in english by default  
 * 
 * 
 * @since 05/07/2013
 */
public class VsCount {
    
    private final Table<String, String, Integer> table;
    
    /** Creates the VsCount **/
    public VsCount(Table<String, String, Integer> arg) {
        this.table = HashBasedTable.create(arg);
    }
    
    /** Creates the VsCount */
    public VsCount() {
        this(HashBasedTable.<String, String, Integer>create());
    }

    public final void update(String fragger, String fragged) {
        Integer base = Objects.firstNonNull(table.get(fragger, fragged), 0);
        table.put(fragger, fragged, base + 1);
    }

    public final VsCount sum(final VsCount other) {
        final Table<String, String, Integer> ret = HashBasedTable.create();
        
        for (final Cell<String, String, Integer> cell : table.cellSet()) {
            ret.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
        }
        
        for (final Cell<String, String, Integer> cell : other.table.cellSet()) {
            final Integer base = Objects.firstNonNull(ret.get(cell.getRowKey(), cell.getColumnKey()), 0);
            ret.put(cell.getRowKey(), cell.getColumnKey(), base + cell.getValue());
        }
        return new VsCount(ret);
    }
    
    public final Table<String, String, Integer> getTarget() {
        return table;
    }

    @Override
    public final String toString() {
        return "VsCount [" + table + "]";
    }

}
