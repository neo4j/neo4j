package org.neo4j.server.rest.web.paging;

import java.util.HashMap;

public class LeaseManager
{
    private static final long ONE_MINUTE_IN_MILLISECONDS = 60 * 1000;
    private final Clock clock;
    private HashMap<String, Lease> leases = new HashMap<String, Lease>();

    public LeaseManager(Clock clock) {
        this.clock = clock;
        
    }

    public Lease createLease()
    {
        return new Lease(null, clock.currentTimeInMilliseconds() + ONE_MINUTE_IN_MILLISECONDS);
    }

    public Lease getLeaseById( String id )
    {
        return leases.get( id );
    }
}
