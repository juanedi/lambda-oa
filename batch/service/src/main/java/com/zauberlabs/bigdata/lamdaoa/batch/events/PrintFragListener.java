/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch.events;

import com.zauberlabs.bigdata.lamdaoa.batch.Frag;

/**
 * {@link FragListener} that prints frag information.  
 * 
 * @since Aug 23, 2013
 */
public class PrintFragListener implements FragListener {

    /** @see FragListener#process(Frag) */
    @Override
    public void process(Frag frag) {
        System.out.println(frag.getTimestamp() + "," + frag.getGame()
                   + "," + frag.getFragger() + "," + frag.getFragged());
    }
    
}