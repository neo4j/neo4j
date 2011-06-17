package org.neo4j.server.rest.web.paging;

import java.util.UUID;

public class Lease<T>
{
    public final long expirationTime;
    public final T t;
    private final String id;

    public Lease( T t, long expirationTime )
    {
        this.t = t;
        this.expirationTime = expirationTime;
        this.id = toHexOnly( UUID.randomUUID() );
    }

    public String getId()
    {
        return id;
    }

    private String toHexOnly( UUID uuid )
    {
        return uuid.toString()
                .replaceAll( "-", "" );
    }
}
