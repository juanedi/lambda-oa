/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.util;

import java.util.Map;


/**
 * Null {@link FragStore}  
 * 
 * 
 * @since 05/07/2013
 */
public class NullFragStore implements FragStore {
    

    @Override
    public void updateFragCount(Map<String, Long> fraggers) {
        System.out.println(fraggers);

    }

    @Override
    public void updateFragVersusCount(VsCount fraggers) {
        System.out.println(fraggers);
    }

}
