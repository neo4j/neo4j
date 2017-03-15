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
package org.neo4j.causalclustering.discovery;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.hamcrest.Matchers;
import org.junit.Test;

import org.neo4j.causalclustering.identity.MemberId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class ReadReplicaTopologyTest
{
    @Test
    public void identicalTopologiesShouldHaveNoDifference() throws Exception
    {
        // given
        UUID one = UUID.randomUUID();
        UUID two = UUID.randomUUID();

        Map<MemberId,ReadReplicaInfo> readReplicaMembers = new HashMap<>();
        readReplicaMembers.put( new MemberId( one ), mock(ReadReplicaInfo.class) );
        readReplicaMembers.put( new MemberId( two ), mock(ReadReplicaInfo.class) );

        ReadReplicaTopology topology = new ReadReplicaTopology( readReplicaMembers );

        // when
        TopologyDifference diff =  topology.difference(topology);

        // then
        assertThat( diff.added().size(), Matchers.equalTo( 0 ) );
        assertThat( diff.removed().size(), Matchers.equalTo( 0 ) );
    }

    @Test
    public void shouldDetectAddedMembers() throws Exception
    {
        // given
        UUID one = UUID.randomUUID();
        UUID two = UUID.randomUUID();

        Map<MemberId,ReadReplicaInfo> initialMembers = new HashMap<>();
        initialMembers.put( new MemberId( one ), mock(ReadReplicaInfo.class) );
        initialMembers.put( new MemberId( two ), mock(ReadReplicaInfo.class) );

        Map<MemberId,ReadReplicaInfo> newMembers = new HashMap<>();
        newMembers.put( new MemberId( one ), mock(ReadReplicaInfo.class) );
        newMembers.put( new MemberId( two ), mock(ReadReplicaInfo.class) );
        newMembers.put( new MemberId( UUID.randomUUID() ), mock(ReadReplicaInfo.class) );

        ReadReplicaTopology topology = new ReadReplicaTopology( initialMembers );

        // when
        TopologyDifference diff =  topology.difference(new ReadReplicaTopology( newMembers ));

        // then
        assertThat( diff.added().size(), Matchers.equalTo( 1 ) );
        assertThat( diff.removed().size(), Matchers.equalTo( 0 ) );
    }

    @Test
    public void shouldDetectRemovedMembers() throws Exception
    {
        // given
        UUID one = UUID.randomUUID();
        UUID two = UUID.randomUUID();

        Map<MemberId,ReadReplicaInfo> initialMembers = new HashMap<>();
        initialMembers.put( new MemberId( one ), mock(ReadReplicaInfo.class) );
        initialMembers.put( new MemberId( two ), mock(ReadReplicaInfo.class) );

        Map<MemberId,ReadReplicaInfo> newMembers = new HashMap<>();
        newMembers.put( new MemberId( two ), mock(ReadReplicaInfo.class) );

        ReadReplicaTopology topology = new ReadReplicaTopology( initialMembers );

        // when
        TopologyDifference diff =  topology.difference(new ReadReplicaTopology( newMembers ));

        // then
        assertThat( diff.added().size(), Matchers.equalTo( 0 ) );
        assertThat( diff.removed().size(), Matchers.equalTo( 1 ) );
    }

    @Test
    public void shouldDetectAddedAndRemovedMembers() throws Exception
    {
        // given

        Map<MemberId,ReadReplicaInfo> initialMembers = new HashMap<>();
        initialMembers.put( new MemberId( UUID.randomUUID() ), mock(ReadReplicaInfo.class) );
        initialMembers.put( new MemberId( UUID.randomUUID() ), mock(ReadReplicaInfo.class) );

        Map<MemberId,ReadReplicaInfo> newMembers = new HashMap<>();
        newMembers.put( new MemberId( UUID.randomUUID() ), mock(ReadReplicaInfo.class) );
        newMembers.put( new MemberId( UUID.randomUUID() ), mock(ReadReplicaInfo.class) );

        ReadReplicaTopology topology = new ReadReplicaTopology( initialMembers );

        // when
        TopologyDifference diff =  topology.difference(new ReadReplicaTopology( newMembers ));

        // then
        assertThat( diff.added().size(), Matchers.equalTo( 2 ) );
        assertThat( diff.removed().size(), Matchers.equalTo( 2 ) );
    }
}
