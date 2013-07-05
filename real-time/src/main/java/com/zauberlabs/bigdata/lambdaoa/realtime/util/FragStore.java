/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lambdaoa.realtime.util;

import java.util.Map;

/**
 * TODO: Description of the class, Comments in english by default  
 * 
 * 
 * @since 05/07/2013
 */
public interface FragStore {
    
    /** Updates global frag counts */
    void updateFragCount(Map<String, Long> fraggers);

    /** Updates vs frags matrix */
    void updateFragVersusCount(VsCount fraggers);
}
