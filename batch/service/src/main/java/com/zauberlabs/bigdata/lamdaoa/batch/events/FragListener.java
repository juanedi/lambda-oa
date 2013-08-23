/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.events;

import com.zauberlabs.bigdata.lamdaoa.batch.Frag;

/**
 * Listener of frag events.
 * 
 * 
 * @since Aug 23, 2013
 */
public interface FragListener {

    void process(Frag frag);
    
}
