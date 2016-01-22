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
package org.neo4j.coreedge.raft.replication.session;

import java.util.UUID;

import org.junit.Test;

import org.neo4j.coreedge.server.RaftTestMember;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class BaseGlobalSessionTrackerStateTest
{
    RaftTestMember coreA = RaftTestMember.member( 1 );
    RaftTestMember coreB = RaftTestMember.member( 2 );

    GlobalSession sessionA = new GlobalSession( UUID.randomUUID(), coreA );
    GlobalSession sessionA2 = new GlobalSession( UUID.randomUUID(), coreA );

    GlobalSession sessionB = new GlobalSession( UUID.randomUUID(), coreB );

    protected abstract GlobalSessionTrackerState<RaftTestMember> instantiateSessionTracker();

    @Test
    public void firstValidSequenceNumberIsZero()
    {
        GlobalSessionTrackerState sessionTracker = instantiateSessionTracker();

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 0 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 1, -1 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 1, -1 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 2, 1 ) ) );
    }

    @Test
    public void repeatedOperationsAreRejected()
    {
        GlobalSessionTrackerState sessionTracker = instantiateSessionTracker();

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 0 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 0 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
    }

    @Test
    public void seriesOfOperationsAreAccepted()
    {
        GlobalSessionTrackerState sessionTracker = instantiateSessionTracker();

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 0 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 1 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 1 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 2 ) ) );
    }

    @Test
    public void gapsAreNotAllowed()
    {
        GlobalSessionTrackerState sessionTracker = instantiateSessionTracker();

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 0 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 1 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 1 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 3 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 3 ), 0 );

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 2 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 2 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 3 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 3 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 4 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 4 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 6 ) ) );
    }

    @Test
    public void localSessionsAreIndependent()
    {
        GlobalSessionTrackerState sessionTracker = instantiateSessionTracker();

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 0 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 0 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 1 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 1 ), 0 );

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 1, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 1, 0 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 1, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 1, 0 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 1, 1 ) ) );
    }

    @Test
    public void globalSessionsAreIndependent()
    {
        GlobalSessionTrackerState sessionTracker = instantiateSessionTracker();

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 0 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionB, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionB, new LocalOperationId( 0, 0 ), 0 );

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 1, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 1, 0 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionB, new LocalOperationId( 1, 0 ) ) );
        sessionTracker.update( sessionB, new LocalOperationId( 1, 0 ), 0 );

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 2, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 2, 0 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 2, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 2, 0 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionB, new LocalOperationId( 2, 0 ) ) );
        sessionTracker.update( sessionB, new LocalOperationId( 2, 0 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionA, new LocalOperationId( 2, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 2, 0 ), 0 );
        assertFalse( sessionTracker.validateOperation( sessionB, new LocalOperationId( 2, 0 ) ) );
    }

    @Test
    public void newGlobalSessionUnderSameOwnerResetsCorrespondingLocalSessionTracker()
    {
        GlobalSessionTrackerState sessionTracker = instantiateSessionTracker();

        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 0 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionA, new LocalOperationId( 0, 1 ) ) );
        sessionTracker.update( sessionA, new LocalOperationId( 0, 1 ), 0 );

        assertFalse( sessionTracker.validateOperation( sessionA2, new LocalOperationId( 0, 2 ) ) );
        sessionTracker.update( sessionA2, new LocalOperationId( 0, 2 ), 0 );

        assertTrue( sessionTracker.validateOperation( sessionA2, new LocalOperationId( 0, 0 ) ) );
        sessionTracker.update( sessionA2, new LocalOperationId( 0, 0 ), 0 );
        assertTrue( sessionTracker.validateOperation( sessionA2, new LocalOperationId( 0, 1 ) ) );
    }
}
