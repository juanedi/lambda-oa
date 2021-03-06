/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch;

import javax.validation.constraints.NotNull;

import com.yammer.dropwizard.config.Configuration;

/**
 * Configuration for {@link BatchJobsService}.  
 * 
 * 
 * @since Jul 5, 2013
 */
public class BatchJobServiceConfiguration extends Configuration {

    @NotNull private String rmqHost;

    public String getRmqHost() {
        return rmqHost;
    }
    
}
