package org.neo4j.server.rest.web.paging;

import static org.junit.Assert.assertThat;
import static org.neo4j.server.rest.web.paging.HexMatcher.containsOnlyHex;

import org.junit.Test;

public class LeaseTest
{
    private long ONE_MINUTE_IN_MILLISECONDS = 60000;

    @Test
    public void shouldReturnHexIdentifierString() throws Exception
    {
        Lease<Leasable> lease = new Lease<Leasable>( new Leasable(){}, oneMinuteFromNow());
        assertThat(lease.getId(), containsOnlyHex());
    }
    
    private long oneMinuteFromNow()
    {
        return ONE_MINUTE_IN_MILLISECONDS + System.currentTimeMillis(); 
    }

    @Test (expected = LeaseAlreadyExpiredException.class)
    public void shouldNotAllowLeasesInThePast() throws Exception {
        new Lease<Leasable>( new Leasable(){}, oneMinuteBeforeNow());
    }
    
    private long oneMinuteBeforeNow()
    {
        return System.currentTimeMillis() - ONE_MINUTE_IN_MILLISECONDS; 
    }
}
