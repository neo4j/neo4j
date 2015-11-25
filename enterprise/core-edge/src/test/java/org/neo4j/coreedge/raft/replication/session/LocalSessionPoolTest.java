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

import org.junit.Test;

import org.neo4j.coreedge.server.CoreMember;

import static org.junit.Assert.*;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class LocalSessionPoolTest
{
    CoreMember coreMember = new CoreMember( address( "core:1" ), address( "raft:1" ) );

    @Test
    public void poolsHaveUniqueGlobalIDs()
    {
        LocalSessionPool sessionPoolA = new LocalSessionPool( coreMember );
        LocalSessionPool sessionPoolB = new LocalSessionPool( coreMember );

        assertNotEquals( sessionPoolA.getGlobalSession(), sessionPoolB.getGlobalSession() );
    }

    @Test
    public void poolGivesBackSameSessionAfterRelease()
    {
        LocalSessionPool sessionPool = new LocalSessionPool( coreMember );

        OperationContext contextA = sessionPool.acquireSession();
        sessionPool.releaseSession( contextA );

        OperationContext contextB = sessionPool.acquireSession();
        sessionPool.releaseSession( contextB );

        assertEquals( contextA.localSession(), contextB.localSession() );
    }

    @Test
    public void sessionAcquirementIncreasesOperationId()
    {
        LocalSessionPool sessionPool = new LocalSessionPool( coreMember );
        OperationContext context;

        context = sessionPool.acquireSession();
        LocalOperationId operationA = context.localOperationId();
        sessionPool.releaseSession( context );

        context = sessionPool.acquireSession();
        LocalOperationId operationB = context.localOperationId();
        sessionPool.releaseSession( context );

        assertEquals( operationB.sequenceNumber(), operationA.sequenceNumber() + 1 );
    }

    @Test
    public void poolHasIndependentSessions()
    {
        LocalSessionPool sessionPool = new LocalSessionPool( coreMember );

        OperationContext contextA = sessionPool.acquireSession();
        OperationContext contextB = sessionPool.acquireSession();

        assertNotEquals( contextA.localSession(), contextB.localSession() );
    }
}
