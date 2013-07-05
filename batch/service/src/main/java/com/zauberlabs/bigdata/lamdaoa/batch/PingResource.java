/*
 * Copyright (c) 2013 MercadoLibre  -- All rights reserved
 */
package com.zauberlabs.bigdata.lamdaoa.batch;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Ping resource.
 * 
 * 
 * @since Jul 5, 2013
 */
@Path("/ping")
public class PingResource {

    @GET
    public final Response ping(@Context final HttpServletRequest request) {
        return Response.status(200)
                        .entity("pong")
                        .type(MediaType.TEXT_PLAIN)
                        .build();
    }
    
}
