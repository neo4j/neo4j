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
package org.neo4j.coreedge.raft.state.explorer;

import org.junit.Test;

import org.neo4j.coreedge.raft.log.InMemoryRaftLog;

import static org.junit.Assert.assertEquals;

import static org.neo4j.coreedge.server.RaftTestMember.member;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class ComparableRaftStateTest
{
    @Test
    public void twoIdenticalStatesShouldBeEqual() throws Exception
    {
        // given
        ComparableRaftState state1 = new ComparableRaftState( member( 0 ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                new InMemoryRaftLog() );

        ComparableRaftState state2 = new ComparableRaftState( member( 0 ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                new InMemoryRaftLog() );

        // then
        assertEquals(state1, state2);
    }
}