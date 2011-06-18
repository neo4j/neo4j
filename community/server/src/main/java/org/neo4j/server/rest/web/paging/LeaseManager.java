package org.neo4j.server.rest.web.paging;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaseManager<T extends Leasable>
{
    private static final long ONE_MINUTE_IN_SECONDS = 60;
    private final Clock clock;
    private Map<String, Lease<T>> leases = new ConcurrentHashMap<String, Lease<T>>();

    public LeaseManager( Clock clock )
    {
        this.clock = clock;
    }

    public Lease<T> createLease( T leasedObject ) throws LeaseAlreadyExpiredException
    {
        return createLease( ONE_MINUTE_IN_SECONDS, leasedObject );
    }

    public Lease<T> createLease( long seconds, T leasedObject ) throws LeaseAlreadyExpiredException
    {
        if ( seconds < 1 )
        {
            return null;
        }

        Lease<T> lease = new Lease<T>( leasedObject, clock.currentTimeInMilliseconds() + (seconds * 1000) );
        leases.put( lease.getId(), lease );
        return lease;
    }

    public Lease<T> getLeaseById( String id )
    {
        pruneOldLeasesByNaivelyIteratingThroughAllOfThem();
        return leases.get( id );
    }

    private void pruneOldLeasesByNaivelyIteratingThroughAllOfThem()
    {
        for ( String key : leases.keySet() )
        {
            try
            {
                Lease<T> lease = leases.get( key );
                if ( lease.expirationTime < clock.currentTimeInMilliseconds() )
                {
                    leases.remove( key );
                }
            }
            catch ( Exception e )
            {
                // do nothing - if something goes wrong, it just means another
                // thread already nuked the lease
            }
        }
    }
}
