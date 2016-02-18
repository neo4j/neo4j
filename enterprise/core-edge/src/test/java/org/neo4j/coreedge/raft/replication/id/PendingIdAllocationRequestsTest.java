/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.replication.id;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.core.locks.PendingIdAllocationRequest;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PendingIdAllocationRequestsTest
{
    CoreMember me = new CoreMember( new AdvertisedSocketAddress( "a:1" ), new AdvertisedSocketAddress( "a:2" ) );

    @Test
    public void shouldStartEmpty() throws Exception
    {
        // given
        ReplicatedIdAllocationRequest request = new ReplicatedIdAllocationRequest( me, IdType.NODE, 0, 1024 );
        PendingIdAllocationRequests requests = new PendingIdAllocationRequests();

        // when
        PendingIdAllocationRequest retrieved = requests.retrieve( request );

        // then
        assertNull( retrieved );
    }

    @Test
    public void shouldRegisterAndRetrieve() throws Exception
    {
        // given
        ReplicatedIdAllocationRequest request = new ReplicatedIdAllocationRequest( me, IdType.NODE, 0, 1024 );
        PendingIdAllocationRequests requests = new PendingIdAllocationRequests();

        // when
        PendingIdAllocationRequest registered = requests.register( request );
        PendingIdAllocationRequest retrieved = requests.retrieve( request );

        // then
        assertSame( registered, retrieved );
    }

    @Test
    public void shouldNotifyAcquired() throws Exception
    {
        // given
        ReplicatedIdAllocationRequest request = new ReplicatedIdAllocationRequest( me, IdType.NODE, 0, 1024 );
        PendingIdAllocationRequests requests = new PendingIdAllocationRequests();
        PendingIdAllocationRequest future = requests.register( request );

        // when
        future.notifyAcquired();

        // then
        assertTrue( future.waitUntilAcquired( 1, TimeUnit.MILLISECONDS ) );
    }

    @Test
    public void shouldNotifyLost() throws Exception
    {
        // given
        ReplicatedIdAllocationRequest request = new ReplicatedIdAllocationRequest( me, IdType.NODE, 0, 1024 );
        PendingIdAllocationRequests requests = new PendingIdAllocationRequests();
        PendingIdAllocationRequest future = requests.register( request );

        // when
        future.notifyLost();

        // then
        assertFalse( future.waitUntilAcquired( 1, TimeUnit.MILLISECONDS ) );
    }
}