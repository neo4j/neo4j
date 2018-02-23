/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class LeaseManagerTest
{
    private static final long SIXTY_SECONDS = 60;

    @Test
    public void shouldNotAcceptLeasesWithNegativeTTL()
    {
        FakeClock fakeClock = Clocks.fakeClock();
        LeaseManager manager = new LeaseManager( fakeClock );
        assertNull( manager.createLease( -1L, mock( PagedTraverser.class ) ) );
        assertNull( manager.createLease( Long.MAX_VALUE + 1, mock( PagedTraverser.class ) ) );
    }

    @Test
    public void shouldRetrieveAnExistingLeaseImmediatelyAfterCreation()
    {
        FakeClock fakeClock = Clocks.fakeClock();
        LeaseManager manager = new LeaseManager( fakeClock );

        Lease lease = manager.createLease( SIXTY_SECONDS, mock( PagedTraverser.class ) );

        assertNotNull( manager.getLeaseById( lease.getId() ) );

    }

    @Test
    public void shouldRetrieveAnExistingLeaseSomeTimeAfterCreation()
    {
        FakeClock fakeClock = Clocks.fakeClock();
        LeaseManager manager = new LeaseManager( fakeClock );

        Lease lease = manager.createLease( 120, mock( PagedTraverser.class ) );

        fakeClock.forward( 1, TimeUnit.MINUTES );

        assertNotNull( manager.getLeaseById( lease.getId() ) );

    }

    @Test
    public void shouldNotRetrieveALeaseAfterItExpired()
    {
        FakeClock fakeClock = Clocks.fakeClock();
        LeaseManager manager = new LeaseManager( fakeClock );

        Lease lease = manager.createLease( SIXTY_SECONDS, mock( PagedTraverser.class ) );

        fakeClock.forward( 2, TimeUnit.MINUTES );

        assertNull( manager.getLeaseById( lease.getId() ) );
    }

    @Test
    public void shouldNotBarfWhenAnotherThreadOrRetrieveRevokesTheLease()
    {
        FakeClock fakeClock = Clocks.fakeClock();
        LeaseManager manager = new LeaseManager( fakeClock );

        Lease leaseA = manager.createLease( SIXTY_SECONDS, mock( PagedTraverser.class ) );
        Lease leaseB = manager.createLease( SIXTY_SECONDS * 3, mock( PagedTraverser.class ) );

        fakeClock.forward( 2, TimeUnit.MINUTES );

        assertNotNull( manager.getLeaseById( leaseB.getId() ) );
        assertNull( manager.getLeaseById( leaseA.getId() ) );
    }

    @Test
    public void shouldRemoveALease()
    {
        FakeClock fakeClock = Clocks.fakeClock();
        LeaseManager manager = new LeaseManager( fakeClock );
        Lease lease = manager.createLease( 101L, mock( PagedTraverser.class ) );

        manager.remove( lease.getId() );

        assertNull( manager.getLeaseById( lease.getId() ) );
    }
}
