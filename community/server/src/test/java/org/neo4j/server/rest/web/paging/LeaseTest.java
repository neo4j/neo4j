package org.neo4j.server.rest.web.paging;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.neo4j.server.rest.web.paging.HexMatcher.*;

public class LeaseTest
{
    @Test
    public void shouldReturnHexIdentifierString()
    {
        Lease lease = new Lease( null, 0 );
        assertThat(lease.getId(), containsOnlyHex());
    }
}
