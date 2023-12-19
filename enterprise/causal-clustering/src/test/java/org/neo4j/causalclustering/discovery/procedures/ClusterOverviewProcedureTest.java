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

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static org.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
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

        coreMembers.put( theLeader, addressesForCore( 0 ) );
        coreMembers.put( follower1, addressesForCore( 1 ) );
        coreMembers.put( follower2, addressesForCore( 2 ) );

        Map<MemberId,ReadReplicaInfo> replicaMembers = new HashMap<>();
        MemberId replica4 = new MemberId( UUID.randomUUID() );
        MemberId replica5 = new MemberId( UUID.randomUUID() );

        replicaMembers.put( replica4, addressesForReadReplica( 4 ) );
        replicaMembers.put( replica5, addressesForReadReplica( 5 ) );

        Map<MemberId,RoleInfo> roleMap = new HashMap<>();
        roleMap.put( theLeader, RoleInfo.LEADER );
        roleMap.put( follower1, RoleInfo.FOLLOWER );
        roleMap.put( follower2, RoleInfo.FOLLOWER );

        when( topologyService.allCoreServers() ).thenReturn( new CoreTopology( null, false, coreMembers ) );
        when( topologyService.allReadReplicas() ).thenReturn( new ReadReplicaTopology( replicaMembers ) );
        when( topologyService.allCoreRoles() ).thenReturn( roleMap );

        ClusterOverviewProcedure procedure =
                new ClusterOverviewProcedure( topologyService, NullLogProvider.getInstance() );

        // when
        final RawIterator<Object[],ProcedureException> members = procedure.apply( null, new Object[0], null );

        assertThat( members.next(), new IsRecord( theLeader.getUuid(), 5000, RoleInfo.LEADER, asSet( "core", "core0" ) ) );
        assertThat( members.next(),
                new IsRecord( follower1.getUuid(), 5001, RoleInfo.FOLLOWER, asSet( "core", "core1" ) ) );
        assertThat( members.next(),
                new IsRecord( follower2.getUuid(), 5002, RoleInfo.FOLLOWER, asSet( "core", "core2" ) ) );

        assertThat( members.next(),
                new IsRecord( replica4.getUuid(), 6004, RoleInfo.READ_REPLICA, asSet( "replica", "replica4" ) ) );
        assertThat( members.next(),
                new IsRecord( replica5.getUuid(), 6005, RoleInfo.READ_REPLICA, asSet( "replica", "replica5" ) ) );

        assertFalse( members.hasNext() );
    }

    class IsRecord extends TypeSafeMatcher<Object[]>
    {
        private final UUID memberId;
        private final int boltPort;
        private final RoleInfo role;
        private final Set<String> groups;
        private final String dbName;

        IsRecord( UUID memberId, int boltPort, RoleInfo role, Set<String> groups, String dbName )
        {
            this.memberId = memberId;
            this.boltPort = boltPort;
            this.role = role;
            this.groups = groups;
            this.dbName = dbName;
        }

        IsRecord( UUID memberId, int boltPort, RoleInfo role, Set<String> groups )
        {
            this.memberId = memberId;
            this.boltPort = boltPort;
            this.role = role;
            this.groups = groups;
            this.dbName = CausalClusteringSettings.database.getDefaultValue();
        }

        @Override
        protected boolean matchesSafely( Object[] record )
        {
            if ( record.length != 5 )
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
            if ( !groups.equals( recordGroups ) )
            {
                return false;
            }

            return dbName.equals( record[4] );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText(
                    "memberId=" + memberId +
                    ", boltPort=" + boltPort +
                    ", role=" + role +
                    ", groups=" + groups +
                    ", database=" + dbName +
                    '}' );
        }
    }
}
