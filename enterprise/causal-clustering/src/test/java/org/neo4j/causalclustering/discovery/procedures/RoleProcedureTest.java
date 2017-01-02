/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery.procedures;

import org.junit.Test;

import org.neo4j.collection.RawIterator;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.ProcedureException;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asList;

public class RoleProcedureTest
{
    @Test
    public void shouldReturnLeader() throws Exception
    {
        // given
        RaftMachine raft = mock( RaftMachine.class );
        when( raft.isLeader() ).thenReturn( true );
        RoleProcedure proc = new CoreRoleProcedure( raft );

        // when
        RawIterator<Object[], ProcedureException> result = proc.apply( null, null );

        // then
        assertEquals( Role.LEADER.name(), single( result )[0]);
    }

    @Test
    public void shouldReturnFollower() throws Exception
    {
        // given
        RaftMachine raft = mock( RaftMachine.class );
        when( raft.isLeader() ).thenReturn( false );
        RoleProcedure proc = new CoreRoleProcedure( raft );

        // when
        RawIterator<Object[], ProcedureException> result = proc.apply( null, null );

        // then
        assertEquals( Role.FOLLOWER.name(), single( result )[0]);
    }

    @Test
    public void shouldReturnReadReplica() throws Exception
    {
        // given
        RoleProcedure proc = new ReadReplicaRoleProcedure();

        // when
        RawIterator<Object[], ProcedureException> result = proc.apply( null, null );

        // then
        assertEquals( Role.READ_REPLICA.name(), single( result )[0]);
    }

    private Object[] single( RawIterator<Object[], ProcedureException> result ) throws ProcedureException
    {
        return Iterators.single( asList( result ).iterator() );
    }
}
