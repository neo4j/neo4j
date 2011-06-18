/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rest.web.paging;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class LeaseManagerTest
{
    private static final long ONE_MINUTE = 60;

    private static class LeasableObject implements Leasable
    {
    }


    @Test
    public void shouldNotAcceptLeasesWithNegativeTTL() throws Exception
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );
        assertNull( manager.createLease( -1l, new LeasableObject() ) );
        assertNull( manager.createLease( Long.MAX_VALUE + 1, new LeasableObject() ) );
    }

    @Test
    public void shouldRetrieveAnExistingLeaseImmediatelyAfterCreation() throws Exception
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );

        Lease<LeasableObject> lease = manager.createLease( ONE_MINUTE, new LeasableObject() );

        assertNotNull( manager.getLeaseById( lease.getId() ) );

    }

    @Test
    public void shouldRetrieveAnExistingLeaseSomeTimeAfterCreation() throws Exception
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );

        Lease<LeasableObject> lease = manager.createLease( 2 * 60, new LeasableObject() );
        fakeClock.forwardMinutes( 1 );

        assertNotNull( manager.getLeaseById( lease.getId() ) );

    }

    @Test
    public void shouldNotRetrieveALeaseAfterItExpired() throws Exception
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );

        Lease<LeasableObject> lease = manager.createLease( ONE_MINUTE, new LeasableObject() );

        fakeClock.forwardMinutes( 2 );

        assertNull( manager.getLeaseById( lease.getId() ) );
    }

    @Test
    public void shouldNotBarfWhenAnotherThreadOrRetrieveRevokesTheLease() throws Exception
    {
        FakeClock fakeClock = new FakeClock();
        LeaseManager<LeasableObject> manager = new LeaseManager<LeasableObject>( fakeClock );

        Lease<LeasableObject> leaseA = manager.createLease( ONE_MINUTE, new LeasableObject() );
        Lease<LeasableObject> leaseB = manager.createLease( ONE_MINUTE * 3, new LeasableObject() );

        fakeClock.forwardMinutes( 2 );

        assertNotNull( manager.getLeaseById( leaseB.getId() ) );
        assertNull( manager.getLeaseById( leaseA.getId() ) );

    }
}
