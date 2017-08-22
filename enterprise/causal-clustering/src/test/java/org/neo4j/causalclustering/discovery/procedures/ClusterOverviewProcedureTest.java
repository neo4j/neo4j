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

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static org.neo4j.causalclustering.discovery.TestTopology.adressesForCore;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ClusterOverviewProcedureTest
{
    @Test
    public void shouldProvideOverviewOfCoreServersAndReadReplicas() throws Exception
    {
        // given
        final CoreTopologyService topologyService = mock( CoreTopologyService.class );

        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MemberId theLeader = new MemberId( UUID.randomUUID() );
        MemberId follower1 = new MemberId( UUID.randomUUID() );
        MemberId follower2 = new MemberId( UUID.randomUUID() );

        coreMembers.put( theLeader, adressesForCore( 0 ) );
        coreMembers.put( follower1, adressesForCore( 1 ) );
        coreMembers.put( follower2, adressesForCore( 2 ) );

        Map<MemberId,ReadReplicaInfo> replicaMembers = new HashMap<>();
        MemberId replica4 = new MemberId( UUID.randomUUID() );
        MemberId replica5 = new MemberId( UUID.randomUUID() );

        replicaMembers.put( replica4, addressesForReadReplica( 4 ) );
        replicaMembers.put( replica5, addressesForReadReplica( 5 ) );

        when( topologyService.coreServers() ).thenReturn( new CoreTopology( null, false, coreMembers ) );
        when( topologyService.readReplicas() ).thenReturn( new ReadReplicaTopology( replicaMembers ) );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( theLeader );

        ClusterOverviewProcedure procedure =
                new ClusterOverviewProcedure( topologyService, leaderLocator, NullLogProvider.getInstance() );

        // when
        final RawIterator<Object[],ProcedureException> members = procedure.apply( null, new Object[0] );

        assertThat( members.next(), new IsRecord( theLeader.getUuid(), 5000, Role.LEADER, asSet( "core", "core0" ) ) );
        assertThat( members.next(),
                new IsRecord( follower1.getUuid(), 5001, Role.FOLLOWER, asSet( "core", "core1" ) ) );
        assertThat( members.next(),
                new IsRecord( follower2.getUuid(), 5002, Role.FOLLOWER, asSet( "core", "core2" ) ) );

        assertThat( members.next(),
                new IsRecord( replica4.getUuid(), 6004, Role.READ_REPLICA, asSet( "replica", "replica4" ) ) );
        assertThat( members.next(),
                new IsRecord( replica5.getUuid(), 6005, Role.READ_REPLICA, asSet( "replica", "replica5" ) ) );

        assertFalse( members.hasNext() );
    }

    class IsRecord extends TypeSafeMatcher<Object[]>
    {
        private final UUID memberId;
        private final int boltPort;
        private final Role role;
        private final Set<String> groups;

        IsRecord( UUID memberId, int boltPort, Role role, Set<String> groups )
        {
            this.memberId = memberId;
            this.boltPort = boltPort;
            this.role = role;
            this.groups = groups;
        }

        @Override
        protected boolean matchesSafely( Object[] record )
        {
            if ( record.length != 4 )
            {
                return false;
            }

            if ( !memberId.toString().equals( record[0] ) )
            {
                return false;
            }

            List<String> boltAddresses = Collections.singletonList( "bolt://localhost:" + boltPort );

            if ( !boltAddresses.equals( record[1] ) )
            {
                return false;
            }

            if ( !role.name().equals( record[2] ) )
            {
                return false;
            }

            Set<String> recordGroups = Iterables.asSet( (List<String>) record[3] );
            return groups.equals( recordGroups );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText(
                    "memberId=" + memberId +
                    ", boltPort=" + boltPort +
                    ", role=" + role +
                    ", groups=" + groups +
                    '}' );
        }
    }
}
