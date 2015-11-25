/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication.session;

import java.util.UUID;

import org.junit.Test;

import org.neo4j.coreedge.server.CoreMember;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class GlobalSessionTrackerTest
{
    CoreMember coreA = new CoreMember( address( "core:1" ), address( "raft:1" ) );
    CoreMember coreB = new CoreMember( address( "core:2" ), address( "raft:2" ) );

    GlobalSession sessionA = new GlobalSession( UUID.randomUUID(), coreA );
    GlobalSession sessionA2 = new GlobalSession( UUID.randomUUID(), coreA );

    GlobalSession sessionB = new GlobalSession( UUID.randomUUID(), coreB );

    @Test
    public void firstValidSequenceNumberIsZero()
    {
        GlobalSessionTracker sessionTracker = new GlobalSessionTracker();

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 1, -1 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 2, 1 ) ) );
    }

    @Test
    public void repeatedOperationsAreRejected()
    {
        GlobalSessionTracker sessionTracker = new GlobalSessionTracker();

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
    }

    @Test
    public void seriesOfOperationsAreAccepted()
    {
        GlobalSessionTracker sessionTracker = new GlobalSessionTracker();

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 1 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 2 ) ) );
    }

    @Test
    public void gapsAreNotAllowed()
    {
        GlobalSessionTracker sessionTracker = new GlobalSessionTracker();

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 1 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 3 ) ) );

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 2 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 3 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 4 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 6 ) ) );
    }

    @Test
    public void localSessionsAreIndependent()
    {
        GlobalSessionTracker sessionTracker = new GlobalSessionTracker();

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 1 ) ) );

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 1, 0 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 1, 0 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 1, 1 ) ) );
    }

    @Test
    public void globalSessionsAreIndependent()
    {
        GlobalSessionTracker sessionTracker = new GlobalSessionTracker();

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionB, new LocalOperationId( 0, 0 ) ) );

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 1, 0 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionB, new LocalOperationId( 1, 0 ) ) );

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 2, 0 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 2, 0 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionB, new LocalOperationId( 2, 0 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 2, 0 ) ) );
        assertFalse( sessionTracker.validateAndTrackOperation( sessionB, new LocalOperationId( 2, 0 ) ) );
    }

    @Test
    public void newGlobalSessionUnderSameOwnerResetsCorrespondingLocalSessionTracker()
    {
        GlobalSessionTracker sessionTracker = new GlobalSessionTracker();

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionA, new LocalOperationId( 0, 1 ) ) );

        assertFalse( sessionTracker.validateAndTrackOperation( sessionA2, new LocalOperationId( 0, 2 ) ) );

        assertTrue( sessionTracker.validateAndTrackOperation( sessionA2, new LocalOperationId( 0, 0 ) ) );
        assertTrue( sessionTracker.validateAndTrackOperation( sessionA2, new LocalOperationId( 0, 1 ) ) );
    }
}
