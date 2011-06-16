package org.neo4j.server.rest.web.paging;

import static org.junit.Assert.assertThat;
import static org.neo4j.server.rest.web.paging.HexMatcher.containsOnlyHex;

import org.junit.Test;

public class LeaseTest
{
    @Test
    public void shouldReturnHexIdentifierString()
    {
        Lease<TraversalPager> lease = new Lease<TraversalPager>( null, 0 );
        assertThat(lease.getId(), containsOnlyHex());
    }
}
