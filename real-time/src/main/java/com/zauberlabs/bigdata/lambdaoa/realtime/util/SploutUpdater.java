/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.util;

import java.util.Map;
import java.util.Map.Entry;

import org.fest.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Table.Cell;
import com.splout.db.common.SploutClient;

/**
 * TODO: Description of the class, Comments in english by default  
 * 
 * 
 * @since 05/07/2013
 */
public class SploutUpdater implements FragStore {
    
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SploutClient client;
    private final String tablespace = "frag_stats";
    
    /** Creates the SploutUpdater. */
    public SploutUpdater(final SploutClient client) {
        this.client = Preconditions.checkNotNull(client);
    }
    
    public final void updateFragCount(final Map<String, Long> fraggers) {
        for (final Entry<String, Long> entry : fraggers.entrySet()) {
            final String fragger = entry.getKey();
            final Long delta= entry.getValue();
            try {
                client.query(tablespace, fragger,
                    "update frags_count set count = count + " + delta + " where fragger = '" + fragger + "'", null);
            } catch (Exception e) {
                logger.error("Error executing update", e);
            }
        }
    }
    
    public final void updateFragVersusCount(final VsCount fraggers) {
        for (final Cell<String, String, Integer> cell : fraggers.getTarget().cellSet()) {
            try {
                final String fragger = cell.getColumnKey();
                final String fragged = cell.getRowKey();
                
                client.query(tablespace, fragger,
                    "update versus_count set count = count + 1 " +
                    "where fragger = '" + fragger +
                    "' and fragged = '" + fragged + "'", null);
            } catch (Exception e) {
                logger.error("Error executing update", e);
            }
        }
    }

}
