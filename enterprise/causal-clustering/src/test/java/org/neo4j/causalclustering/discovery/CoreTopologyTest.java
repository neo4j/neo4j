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

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class CoreTopologyTest
{
    @Test
    public void identicalTopologiesShouldHaveNoDifference() throws Exception
    {
        // given
        UUID one = UUID.randomUUID();
        UUID two = UUID.randomUUID();

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( new MemberId( one ), mock(CoreServerInfo.class) );
        coreMembers.put( new MemberId( two ), mock(CoreServerInfo.class) );

        CoreTopology topology = new CoreTopology( new ClusterId( UUID.randomUUID() ), true, coreMembers );

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

        Map<MemberId,CoreServerInfo> initialMembers = new HashMap<>();
        initialMembers.put( new MemberId( one ), mock(CoreServerInfo.class) );
        initialMembers.put( new MemberId( two ), mock(CoreServerInfo.class) );

        Map<MemberId,CoreServerInfo> newMembers = new HashMap<>();
        newMembers.put( new MemberId( one ), mock(CoreServerInfo.class) );
        newMembers.put( new MemberId( two ), mock(CoreServerInfo.class) );
        newMembers.put( new MemberId( UUID.randomUUID() ), mock(CoreServerInfo.class) );

        CoreTopology topology = new CoreTopology( new ClusterId( UUID.randomUUID() ), true, initialMembers );

        // when
        TopologyDifference diff =  topology.difference(new CoreTopology( new ClusterId( UUID.randomUUID() ), true, newMembers ));

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

        Map<MemberId,CoreServerInfo> initialMembers = new HashMap<>();
        initialMembers.put( new MemberId( one ), mock(CoreServerInfo.class) );
        initialMembers.put( new MemberId( two ), mock(CoreServerInfo.class) );

        Map<MemberId,CoreServerInfo> newMembers = new HashMap<>();
        newMembers.put( new MemberId( two ), mock(CoreServerInfo.class) );

        CoreTopology topology = new CoreTopology( new ClusterId( UUID.randomUUID() ), true, initialMembers );

        // when
        TopologyDifference diff =  topology.difference(new CoreTopology( new ClusterId( UUID.randomUUID() ), true, newMembers ));

        // then
        assertThat( diff.added().size(), Matchers.equalTo( 0 ) );
        assertThat( diff.removed().size(), Matchers.equalTo( 1 ) );
    }

    @Test
    public void shouldDetectAddedAndRemovedMembers() throws Exception
    {
        // given

        Map<MemberId,CoreServerInfo> initialMembers = new HashMap<>();
        initialMembers.put( new MemberId( UUID.randomUUID() ), mock(CoreServerInfo.class) );
        initialMembers.put( new MemberId( UUID.randomUUID() ), mock(CoreServerInfo.class) );

        Map<MemberId,CoreServerInfo> newMembers = new HashMap<>();
        newMembers.put( new MemberId( UUID.randomUUID() ), mock(CoreServerInfo.class) );
        newMembers.put( new MemberId( UUID.randomUUID() ), mock(CoreServerInfo.class) );

        CoreTopology topology = new CoreTopology( new ClusterId( UUID.randomUUID() ), true, initialMembers );

        // when
        TopologyDifference diff =  topology.difference(new CoreTopology( new ClusterId( UUID.randomUUID() ), true, newMembers ));

        // then
        assertThat( diff.added().size(), Matchers.equalTo( 2 ) );
        assertThat( diff.removed().size(), Matchers.equalTo( 2 ) );
    }
}
