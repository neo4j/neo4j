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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.SharedDiscoveryService;
import org.neo4j.causalclustering.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.causalclustering.discovery.procedures.Role;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.causalclustering.discovery.procedures.Role.FOLLOWER;
import static org.neo4j.causalclustering.discovery.procedures.Role.LEADER;
import static org.neo4j.causalclustering.discovery.procedures.Role.READ_REPLICA;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith( Parameterized.class )
public class ClusterOverviewIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" );

    private enum DiscoveryService
    {
        SHARED,
        HAZELCAST
    }

    @Parameterized.Parameters( name = "discovery-{0}" )
    public static Collection<DiscoveryService> data()
    {
        return Arrays.asList( DiscoveryService.SHARED, DiscoveryService.HAZELCAST );
    }

    public ClusterOverviewIT( DiscoveryService discoveryService )
    {
        switch ( discoveryService )
        {
        case SHARED:
            clusterRule.withDiscoveryServiceFactory( new SharedDiscoveryService() );
            break;
        case HAZELCAST:
            clusterRule.withDiscoveryServiceFactory( new HazelcastDiscoveryServiceFactory() );
            break;
        default:
            throw new IllegalArgumentException();
        }
    }

    @Test
    public void shouldDiscoverCoreMembers() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfReadReplicas( 0 );

        // when
        Cluster cluster = clusterRule.startCluster();

        Matcher<List<MemberInfo>> expected = allOf(
                containsMemberAddresses( cluster.coreMembers() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, 2 ), doesNotContainRole( READ_REPLICA ) );

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, expected, coreServerId );
        }
    }

    @Test
    public void shouldDiscoverCoreMembersAndReadReplicas() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfReadReplicas( 3 );

        // when
        Cluster cluster = clusterRule.startCluster();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, 2 ), containsRole( READ_REPLICA, 3 ) );

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, expected, coreServerId );
        }
    }

    @Test
    public void shouldDiscoverReadReplicasAfterRestartingCores() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfReadReplicas( 3 );

        // when
        Cluster cluster = clusterRule.startCluster();
        cluster.shutdownCoreMembers();
        cluster.startCoreMembers();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, 2 ), containsRole( READ_REPLICA, 3 ) );

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, expected, coreServerId );
        }
    }

    @Test
    public void shouldDiscoverNewCoreMembers() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfReadReplicas( 0 );

        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.addCoreMemberWithId( 3 ).start();
        cluster.addCoreMemberWithId( 4 ).start();

        Matcher<List<MemberInfo>> expected = allOf(
                containsMemberAddresses( cluster.coreMembers() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, 4 ) );

        for ( int coreServerId = 0; coreServerId < 5; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, expected, coreServerId );
        }
    }

    @Test
    public void shouldDiscoverNewReadReplicas() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfReadReplicas( 3 );

        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.addReadReplicaWithId( 3 ).start();
        cluster.addReadReplicaWithId( 4 ).start();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAllMemberAddresses( cluster.coreMembers(), cluster.readReplicas() ),
                containsRole( LEADER, 1 ), containsRole( FOLLOWER, 2 ), containsRole( READ_REPLICA, 5 ) );

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, expected, coreServerId );
        }
    }

    @Test
    public void shouldDiscoverRemovalOfReadReplicas() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfReadReplicas( 3 );

        Cluster cluster = clusterRule.startCluster();

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            assertEventualOverview( cluster, containsRole( READ_REPLICA, 3 ), coreServerId );
        }

        // when
        cluster.removeReadReplicaWithMemberId( 0 );
        cluster.removeReadReplicaWithMemberId( 1 );

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, containsRole( READ_REPLICA, 1 ), coreServerId );
        }
    }

    @Test
    public void shouldDiscoverRemovalOfCoreMembers() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 5 );
        clusterRule.withNumberOfReadReplicas( 0 );

        Cluster cluster = clusterRule.startCluster();

        for ( int coreServerId = 0; coreServerId < 5; coreServerId++ )
        {
            assertEventualOverview( cluster, allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, 4 ) ),
                    coreServerId );
        }

        // when
        cluster.removeCoreMemberWithMemberId( 0 );
        cluster.removeCoreMemberWithMemberId( 1 );

        for ( int coreServerId = 2; coreServerId < 5; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, allOf( containsRole( LEADER, 1 ), containsRole( FOLLOWER, 2 ) ),
                    coreServerId );
        }
    }

    private void assertEventualOverview( Cluster cluster, Matcher<List<MemberInfo>> expected, int coreServerId )
            throws KernelException, InterruptedException
    {
        assertEventually( "should have overview from core " + coreServerId,
                () -> clusterOverview( cluster.getCoreMemberById( coreServerId ).database() ), expected, 60, SECONDS );
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

    private Matcher<List<MemberInfo>> containsRole( Role expectedRole, long expectedCount )
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

    private Matcher<List<MemberInfo>> doesNotContainRole( Role unexpectedRole )
    {
       return containsRole( unexpectedRole, 0 );
    }

    @SuppressWarnings( "unchecked" )
    private List<MemberInfo> clusterOverview( GraphDatabaseFacade db )
            throws TransactionFailureException, ProcedureException
    {
        InwardKernel kernel = db.getDependencyResolver().resolveDependency( InwardKernel.class );
        KernelTransaction transaction = kernel.newTransaction( Type.implicit, AnonymousContext.read() );
        List<MemberInfo> infos = new ArrayList<>();
        try ( Statement statement = transaction.acquireStatement() )
        {
            RawIterator<Object[],ProcedureException> itr = statement.procedureCallOperations().procedureCallRead(
                    procedureName( "dbms", "cluster", ClusterOverviewProcedure.PROCEDURE_NAME ), null );

            while ( itr.hasNext() )
            {
                Object[] row = itr.next();
                List<String> addresses = (List<String>) row[1];
                infos.add( new MemberInfo( addresses.toArray( new String[addresses.size()] ),
                        Role.valueOf( (String) row[2] ) ) );
            }
        }

        return infos;
    }

    private static class MemberInfo
    {
        private final String[] addresses;
        private final Role role;

        MemberInfo( String[] addresses, Role role )
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
            return Objects.hash( addresses, role );
        }

        @Override
        public String toString()
        {
            return String.format( "MemberInfo{addresses='%s', role=%s}", Arrays.toString( addresses ), role );
        }
    }
}
