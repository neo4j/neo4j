/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.paging;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.server.rest.paging.HexMatcher.containsOnlyHex;

public class LeaseTest
{
    private static final long SIXTY_SECONDS = 60;

    @Test
    public void shouldReturnHexIdentifierString()
    {
        Lease lease = new Lease( mock( PagedTraverser.class ), SIXTY_SECONDS, Clocks.fakeClock() );
        assertThat( lease.getId(), containsOnlyHex() );
    }

    @Test( expected = LeaseAlreadyExpiredException.class )
    public void shouldNotAllowLeasesInThePast()
    {
        FakeClock clock = Clocks.fakeClock();
        new Lease( mock( PagedTraverser.class ), oneMinuteInThePast(), clock );
    }

    private long oneMinuteInThePast()
    {
        return SIXTY_SECONDS * -1;
    }

    @Test
    public void leasesShouldExpire()
    {
        FakeClock clock = Clocks.fakeClock();
        Lease lease = new Lease( mock( PagedTraverser.class ), SIXTY_SECONDS, clock );
        clock.forward( 10, TimeUnit.MINUTES );
        assertTrue( lease.expired() );
    }

    @Test
    public void shouldRenewLeaseForSamePeriod()
    {
        FakeClock clock = Clocks.fakeClock();
        Lease lease = new Lease( mock( PagedTraverser.class ), SIXTY_SECONDS, clock );

        clock.forward( 30, TimeUnit.SECONDS );

        lease.getLeasedItemAndRenewLease(); // has side effect of renewing the
                                            // lease

        clock.forward( 30, TimeUnit.SECONDS );
        assertFalse( lease.expired() );

        clock.forward( 10, TimeUnit.MINUTES );
        assertTrue( lease.expired() );
    }
}
