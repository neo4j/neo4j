/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.discovery;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Test;

import org.neo4j.causalclustering.identity.MemberId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class TopologyTest
{
    @Test
    public void identicalTopologiesShouldHaveNoDifference()
    {
        // given
        Map<MemberId,ReadReplicaInfo> readReplicaMembers = randomMembers( 5 );

        TestTopology topology = new TestTopology( readReplicaMembers );

        // when
        TopologyDifference diff =  topology.difference(topology);

        // then
        assertThat( diff.added(), hasSize( 0 ) );
        assertThat( diff.removed(), hasSize( 0 ) );
    }

    @Test
    public void shouldDetectAddedMembers()
    {
        // given
        Map<MemberId,ReadReplicaInfo> initialMembers = randomMembers( 3 );

        Map<MemberId,ReadReplicaInfo> newMembers = new HashMap<>( initialMembers );
        int newMemberQuantity = 2;
        IntStream.range( 0, newMemberQuantity )
                .forEach( ignored -> putRandomMember( newMembers ) );

        TestTopology topology = new TestTopology( initialMembers );

        // when
        TopologyDifference diff =  topology.difference(new TestTopology( newMembers ));

        // then
        assertThat( diff.added(), hasSize( newMemberQuantity ) );
        assertThat( diff.removed(), hasSize( 0 ) );
    }

    @Test
    public void shouldDetectRemovedMembers()
    {
        Map<MemberId,ReadReplicaInfo> initialMembers = randomMembers( 3 );

        Map<MemberId,ReadReplicaInfo> newMembers = new HashMap<>( initialMembers );
        int removedMemberQuantity = 2;
        IntStream.range( 0, removedMemberQuantity )
                .forEach( ignored -> removeArbitraryMember( newMembers ) );

        TestTopology topology = new TestTopology( initialMembers );

        // when
        TopologyDifference diff =  topology.difference(new TestTopology( newMembers ));

        // then
        assertThat( diff.added(), hasSize( 0 ) );
        assertThat( diff.removed(), hasSize( removedMemberQuantity ) );
    }

    @Test
    public void shouldDetectAddedAndRemovedMembers()
    {
        // given
        int initialQuantity = 4;
        int newQuantity = 5;
        Map<MemberId,ReadReplicaInfo> initialMembers = randomMembers( initialQuantity );
        Map<MemberId,ReadReplicaInfo> newMembers = randomMembers( newQuantity );

        TestTopology topology = new TestTopology( initialMembers );

        // when
        TopologyDifference diff =  topology.difference(new TestTopology( newMembers ));

        // then
        assertThat( diff.added(), hasSize( newQuantity ) );
        assertThat( diff.removed(), hasSize( initialQuantity ) );
    }

    private static class TestTopology implements Topology<ReadReplicaInfo>
    {
        private final Map<MemberId,ReadReplicaInfo> members;

        private TestTopology( Map<MemberId,ReadReplicaInfo> members )
        {
            this.members = members;
        }

        @Override
        public Map<MemberId,ReadReplicaInfo> members()
        {
            return members;
        }

        @Override
        public Topology<ReadReplicaInfo> filterTopologyByDb( String dbName )
        {
            Map<MemberId, ReadReplicaInfo> newMembers = this.members.entrySet().stream()
                    .filter( e -> e.getValue().getDatabaseName().equals( dbName ) )
                    .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
            return new TestTopology( newMembers );
        }
    }

    private Map<MemberId,ReadReplicaInfo> randomMembers( int quantity )
    {
        return Stream.generate( UUID::randomUUID )
                .limit( quantity )
                .collect( Collectors.toMap( MemberId::new, ignored -> mock(ReadReplicaInfo.class) ) );

    }

    private void putRandomMember( Map<MemberId,ReadReplicaInfo> newmembers )
    {
        newmembers.put( new MemberId( UUID.randomUUID() ), mock(ReadReplicaInfo.class) );
    }

    private void removeArbitraryMember( Map<MemberId,ReadReplicaInfo> members )
    {
        members.remove(
                members.keySet().stream().findAny()
                        .orElseThrow( () -> new AssertionError( "Removing members of an empty map" ) )
        );
    }
}
