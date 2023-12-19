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
package org.neo4j.causalclustering.scenarios;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.neo4j.causalclustering.discovery.RoleInfo.FOLLOWER;
import static org.neo4j.causalclustering.discovery.RoleInfo.LEADER;
import static org.neo4j.causalclustering.discovery.RoleInfo.READ_REPLICA;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith( Parameterized.class )
public class ClusterOverviewIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" );

    @Parameterized.Parameters( name = "discovery-{0}" )
    public static Collection<DiscoveryServiceType> data()
    {
        return Arrays.asList( DiscoveryServiceType.values() );
    }

    public ClusterOverviewIT( DiscoveryServiceType discoveryServiceType )
    {
        clusterRule.withDiscoveryServiceType( discoveryServiceType );
    }

    @Test
    public void shouldDiscoverCoreMembers() throws Exception
    {
        // given
        int coreMembers = 3;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( 0 );

        // when
        Cluster cluster = clusterRule.startCluster();

        Matcher<List<MemberInfo>> expected = allOf(
                containsMemberAddresses( cluster.coreMembers() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 ), doesNotContainRole( READ_REPLICA ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverCoreMembersAndReadReplicas() throws Exception
    {
        // given
        int coreMembers = 3;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        int replicaCount = 3;
        clusterRule.withNumberOfReadReplicas( replicaCount );

        // when
        Cluster cluster = clusterRule.startCluster();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, 2 ), containsRole( READ_REPLICA, replicaCount ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverReadReplicasAfterRestartingCores() throws Exception
    {
        // given
        int coreMembers = 3;
        int readReplicas = 3;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( readReplicas );

        // when
        Cluster cluster = clusterRule.startCluster();
        cluster.shutdownCoreMembers();
        cluster.startCoreMembers();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 ), containsRole( READ_REPLICA, readReplicas ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverNewCoreMembers() throws Exception
    {
        // given
        int initialCoreMembers = 3;
        clusterRule.withNumberOfCoreMembers( initialCoreMembers );
        clusterRule.withNumberOfReadReplicas( 0 );

        Cluster cluster = clusterRule.startCluster();

        // when
        int extraCoreMembers = 2;
        int finalCoreMembers = initialCoreMembers + extraCoreMembers;
        IntStream.range( 0, extraCoreMembers ).forEach( idx -> cluster.addCoreMemberWithId( initialCoreMembers + idx ).start() );

        Matcher<List<MemberInfo>> expected = allOf(
                containsMemberAddresses( cluster.coreMembers() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, finalCoreMembers - 1 ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverNewReadReplicas() throws Exception
    {
        // given
        int coreMembers = 3;
        int initialReadReplicas = 3;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( initialReadReplicas );

        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.addReadReplicaWithId( 3 ).start();
        cluster.addReadReplicaWithId( 4 ).start();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                containsRole( LEADER, 1 ),
                containsRole( FOLLOWER, coreMembers - 1 ),
                containsRole( READ_REPLICA, initialReadReplicas + 2 ) );

        // then
        assertAllEventualOverviews( cluster, expected );
    }

    @Test
    public void shouldDiscoverRemovalOfReadReplicas() throws Exception
    {
        // given
        int coreMembers = 3;
        int initialReadReplicas = 3;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( initialReadReplicas );

        Cluster cluster = clusterRule.startCluster();

        assertAllEventualOverviews( cluster, containsRole( READ_REPLICA, initialReadReplicas ) );

        // when
        cluster.removeReadReplicaWithMemberId( 0 );
        cluster.removeReadReplicaWithMemberId( 1 );

        // then
        assertAllEventualOverviews( cluster, containsRole( READ_REPLICA, initialReadReplicas - 2 ) );
    }

    @Test
    public void shouldDiscoverRemovalOfCoreMembers() throws Exception
    {
        // given
        int coreMembers = 5;
        clusterRule.withNumberOfCoreMembers( coreMembers );
        clusterRule.withNumberOfReadReplicas( 0 );

        Cluster cluster = clusterRule.startCluster();

        assertAllEventualOverviews( cluster, allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 ) ) );

        // when
        cluster.removeCoreMemberWithServerId( 0 );
        cluster.removeCoreMemberWithServerId( 1 );

        // then
        assertAllEventualOverviews( cluster, allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, coreMembers - 1 - 2 ) ),
                asSet( 0, 1 ), Collections.emptySet() );
    }

    @Test
    public void shouldDiscoverTimeoutBasedLeaderStepdown() throws Exception
    {
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfReadReplicas( 2 );

        Cluster cluster = clusterRule.startCluster();
        List<CoreClusterMember> followers = cluster.getAllMembersWithRole( Role.FOLLOWER );
        CoreClusterMember leader = cluster.getMemberWithRole( Role.LEADER );
        followers.forEach( CoreClusterMember::shutdown );

        assertEventualOverview( containsRole( LEADER, 0 ), leader, "core" );
    }

    @Test
    public void shouldDiscoverGreaterTermBasedLeaderStepdown() throws Exception
    {
        int originalCoreMembers = 3;
        clusterRule.withNumberOfCoreMembers( originalCoreMembers );

        Cluster cluster = clusterRule.startCluster();
        CoreClusterMember leader = cluster.awaitLeader();
        leader.config().augment( CausalClusteringSettings.refuse_to_be_leader, Settings.TRUE );

        List<MemberInfo> preElectionOverview = clusterOverview( leader.database() );

        CoreClusterMember follower = cluster.getMemberWithRole( Role.FOLLOWER );
        follower.raft().triggerElection( Clock.systemUTC() );

        assertEventualOverview( allOf(
                containsRole( LEADER, 1 ),
                containsRole( FOLLOWER, originalCoreMembers - 1 ),
                not( equalTo( preElectionOverview ) ) ), leader, "core" );
    }

    private void assertAllEventualOverviews( Cluster cluster, Matcher<List<MemberInfo>> expected ) throws KernelException, InterruptedException
    {
        assertAllEventualOverviews( cluster, expected, Collections.emptySet(), Collections.emptySet()  );
    }

    private void assertAllEventualOverviews( Cluster cluster, Matcher<List<MemberInfo>> expected, Set<Integer> excludedCores, Set<Integer> excludedRRs )
            throws KernelException, InterruptedException
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            if ( !excludedCores.contains( core.serverId() ) )
            {
                assertEventualOverview( expected, core, "core" );
            }

        }

        for ( ReadReplica rr : cluster.readReplicas() )
        {
            if ( !excludedRRs.contains( rr.serverId() ) )
            {
                assertEventualOverview( expected, rr, "rr" );
            }
        }
    }

    private void assertEventualOverview( Matcher<List<MemberInfo>> expected, ClusterMember<? extends GraphDatabaseFacade> member, String role )
            throws KernelException, InterruptedException
    {
        Function<List<MemberInfo>, String> printableMemberInfos =
                memberInfos -> memberInfos.stream().map( MemberInfo::toString ).collect( Collectors.joining( ", " ) );

        String message = String.format( "should have overview from %s %s, but view was ", role, member.serverId() );
        assertEventually( memberInfos -> message + printableMemberInfos.apply( memberInfos ),
                () -> clusterOverview( member.database() ), expected, 90, SECONDS );
    }

    @SafeVarargs
    private final Matcher<Iterable<? extends MemberInfo>> containsAllMemberAddresses(
            Collection<? extends ClusterMember>... members )
    {
        return containsMemberAddresses( Stream.of( members).flatMap( Collection::stream ).collect( toList() ) );
    }

    private Matcher<Iterable<? extends MemberInfo>> containsMemberAddresses( Collection<? extends ClusterMember> members )
    {
        return containsInAnyOrder( members.stream().map( coreClusterMember ->
                new TypeSafeMatcher<MemberInfo>()
                {
                    @Override
                    protected boolean matchesSafely( MemberInfo item )
                    {
                        Set<String> addresses = asSet(item.addresses);
                        for ( URI uri : coreClusterMember.clientConnectorAddresses().uriList() )
                        {
                            if ( !addresses.contains( uri.toString() ) )
                            {
                                return false;
                            }
                        }
                        return true;
                    }

                    @Override
                    public void describeTo( Description description )
                    {
                        description.appendText( "MemberInfo with addresses: " )
                                .appendValue( coreClusterMember.clientConnectorAddresses().boltAddress() );
                    }
                }
        ).collect( toList() ) );
    }

    private Matcher<List<MemberInfo>> containsRole( RoleInfo expectedRole, long expectedCount )
    {
        return new FeatureMatcher<List<MemberInfo>,Long>( equalTo( expectedCount ), expectedRole.name(), "count" )
        {
            @Override
            protected Long featureValueOf( List<MemberInfo> overview )
            {
                return overview.stream().filter( info -> info.role == expectedRole ).count();
            }
        };
    }

    private Matcher<List<MemberInfo>> doesNotContainRole( RoleInfo unexpectedRole )
    {
       return containsRole( unexpectedRole, 0 );
    }

    @SuppressWarnings( "unchecked" )
    private List<MemberInfo> clusterOverview( GraphDatabaseFacade db )
            throws TransactionFailureException, ProcedureException
    {
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );

        List<MemberInfo> infos = new ArrayList<>();
        try ( Session session = kernel.beginSession( AnonymousContext.read() ); Transaction tx = session.beginTransaction( Type.implicit ) )
        {
            RawIterator<Object[],ProcedureException> itr =
                    tx.procedures().procedureCallRead( procedureName( "dbms", "cluster", ClusterOverviewProcedure.PROCEDURE_NAME ), null );

            while ( itr.hasNext() )
            {
                Object[] row = itr.next();
                List<String> addresses = (List<String>) row[1];
                infos.add( new MemberInfo( addresses.toArray( new String[addresses.size()] ), RoleInfo.valueOf( (String) row[2] ) ) );
            }
            return infos;
        }
    }

    private static class MemberInfo
    {
        private final String[] addresses;
        private final RoleInfo role;

        MemberInfo( String[] addresses, RoleInfo role )
        {
            this.addresses = addresses;
            this.role = role;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            MemberInfo that = (MemberInfo) o;
            return Arrays.equals( addresses, that.addresses ) && role == that.role;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( Arrays.hashCode( addresses ), role );
        }

        @Override
        public String toString()
        {
            return String.format( "MemberInfo{addresses='%s', role=%s}", Arrays.toString( addresses ), role );
        }
    }
}
