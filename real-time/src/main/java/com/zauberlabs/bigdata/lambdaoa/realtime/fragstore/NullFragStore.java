/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.fragstore;

import java.util.Map;

import com.zauberlabs.bigdata.lambdaoa.realtime.util.VsCount;


/**
 * Null {@link FragStore}  
 * 
 * 
 * @since 05/07/2013
 */
public class NullFragStore implements FragStore {
    
    @Override
    public void updateFragCount(Map<String, Long> fraggers) {

    }

    @Override
    public void updateFragVersusCount(VsCount fraggers) {
        
    }

}
