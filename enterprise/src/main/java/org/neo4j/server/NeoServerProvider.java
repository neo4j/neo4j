package org.neo4j.server;

import javax.ws.rs.ext.Provider;

import org.neo4j.server.database.AbstractInjectableProvider;

import com.sun.jersey.api.core.HttpContext;

@Provider
public class NeoServerProvider extends
        AbstractInjectableProvider<NeoServer>
{
    public NeoServer neoServer;

    public NeoServerProvider( NeoServer db )
    {
        super( NeoServer.class );
        this.neoServer = db;
    }

    @Override
    public NeoServer getValue( HttpContext httpContext )
    {
        return neoServer;
    }
}