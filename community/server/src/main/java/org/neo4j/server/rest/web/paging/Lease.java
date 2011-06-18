package org.neo4j.server.rest.web.paging;

import java.util.UUID;

public class Lease<T extends Leasable>
{
    public final long expirationTime;
    public final T leasedItem;
    private final String id;

    Lease( T leasedItem, long absoluteExpirationTimeInMilliseconds ) throws LeaseAlreadyExpiredException
    {
        if ( absoluteExpirationTimeInMilliseconds - System.currentTimeMillis() < 0 )
        {
            throw new LeaseAlreadyExpiredException( String.format(
                    "Trying to create a lease [%d] milliseconds in the past is not permitted",
                    absoluteExpirationTimeInMilliseconds - System.currentTimeMillis() ) );
        }

        this.leasedItem = leasedItem;
        this.expirationTime = absoluteExpirationTimeInMilliseconds;
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

    public T getLeasedItem()
    {
        return leasedItem;
    }
}
