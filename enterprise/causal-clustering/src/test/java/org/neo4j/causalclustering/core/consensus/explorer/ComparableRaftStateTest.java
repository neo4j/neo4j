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
package org.neo4j.causalclustering.core.consensus.explorer;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ComparableRaftStateTest
{
    @Test
    public void twoIdenticalStatesShouldBeEqual() throws Exception
    {
        // given
        NullLogProvider logProvider = NullLogProvider.getInstance();
        ComparableRaftState state1 = new ComparableRaftState( member( 0 ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                new InMemoryRaftLog(), new ConsecutiveInFlightCache(), logProvider );

        ComparableRaftState state2 = new ComparableRaftState( member( 0 ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                asSet( member( 0 ), member( 1 ), member( 2 ) ),
                new InMemoryRaftLog(), new ConsecutiveInFlightCache(), logProvider );

        // then
        assertEquals(state1, state2);
    }
}
