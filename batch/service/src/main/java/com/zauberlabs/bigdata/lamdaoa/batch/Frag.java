/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch;

import java.util.Date;

/**
 * Frag event.
 * 
 * 
 * @since Aug 23, 2013
 */
public class Frag {

    private Date timeStamp;
    private String game;
    private String fragger;
    private String fragged;
    
    public Frag(Date timeStamp, String game, String fragger, String fragged) {
        this.timeStamp = timeStamp;
        this.game = game;
        this.fragger = fragger;
        this.fragged = fragged;
    }

    public Date getTimestamp() {
        return timeStamp;
    }
    
    public String getGame() {
        return game;
    }
    
    public String getFragger() {
        return fragger;
    }
    
    public String getFragged() {
        return fragged;
    }
    
}
