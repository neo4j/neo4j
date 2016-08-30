/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.scenarios;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.neo4j.collection.RawIterator;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.coreedge.discovery.SharedDiscoveryService;
import org.neo4j.coreedge.discovery.procedures.ClusterOverviewProcedure;
import org.neo4j.coreedge.discovery.procedures.Role;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.coreedge.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.security.AccessMode.Static.READ;
import static org.neo4j.test.assertion.Assert.assertEventually;

@SuppressWarnings( "unchecked" )
@RunWith( Parameterized.class )
public class ClusterOverviewIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() );

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
        clusterRule.withNumberOfEdgeMembers( 0 );

        // when
        Cluster cluster = clusterRule.startCluster();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAddress( "127.0.0.1:8000" ), containsAddress( "127.0.0.1:8001" ), containsAddress( "127.0.0.1:8002" ),
                containsRole( Role.LEADER, 1 ), containsRole( Role.FOLLOWER, 2 ), doesNotContainRole( Role.READ_REPLICA ) );

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, expected, coreServerId );
        }
    }

    @Test
    public void shouldDiscoverCoreAndEdgeMembers() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfEdgeMembers( 3 );

        // when
        Cluster cluster = clusterRule.startCluster();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAddress( "127.0.0.1:8000" ), containsAddress( "127.0.0.1:8001" ), containsAddress( "127.0.0.1:8002" ),
                containsAddress( "127.0.0.1:9000" ), containsAddress( "127.0.0.1:9001" ), containsAddress( "127.0.0.1:9002" ),
                containsRole( Role.LEADER, 1 ), containsRole( Role.FOLLOWER, 2 ), containsRole( Role.READ_REPLICA, 3 ) );

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
        clusterRule.withNumberOfEdgeMembers( 0 );

        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.addCoreMemberWithId( 3 ).start();
        cluster.addCoreMemberWithId( 4 ).start();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAddress( "127.0.0.1:8000" ), containsAddress( "127.0.0.1:8001" ), containsAddress( "127.0.0.1:8002" ),
                containsRole( Role.LEADER, 1 ), containsRole( Role.FOLLOWER, 4 ),
                containsAddress( "127.0.0.1:8003" ), containsAddress( "127.0.0.1:8004" ) ); // new core members

        for ( int coreServerId = 0; coreServerId < 5; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, expected, coreServerId );
        }
    }

    @Test
    public void shouldDiscoverNewEdgeMembers() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfEdgeMembers( 3 );

        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.addEdgeMemberWithId( 3 ).start();
        cluster.addEdgeMemberWithId( 4 ).start();

        Matcher<List<MemberInfo>> expected = allOf(
                containsAddress( "127.0.0.1:8000" ), containsAddress( "127.0.0.1:8001" ), containsAddress( "127.0.0.1:8002" ),
                containsAddress( "127.0.0.1:9000" ), containsAddress( "127.0.0.1:9001" ), containsAddress( "127.0.0.1:9002" ),
                containsRole( Role.LEADER, 1 ), containsRole( Role.FOLLOWER, 2 ), containsRole( Role.READ_REPLICA, 5 ),
                containsAddress( "127.0.0.1:9003" ), containsAddress( "127.0.0.1:9004" ) ); // new edge members

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, expected, coreServerId );
        }
    }

    @Test
    public void shouldDiscoverRemovalOfEdgeMembers() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 3 );
        clusterRule.withNumberOfEdgeMembers( 3 );

        Cluster cluster = clusterRule.startCluster();

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            assertEventualOverview( cluster, containsRole( Role.READ_REPLICA, 3 ), coreServerId );
        }

        // when
        cluster.removeEdgeMemberWithMemberId( 0 );
        cluster.removeEdgeMemberWithMemberId( 1 );

        for ( int coreServerId = 0; coreServerId < 3; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, containsRole( Role.READ_REPLICA, 1 ), coreServerId );
        }
    }

    @Test
    public void shouldDiscoverRemovalOfCoreMembers() throws Exception
    {
        // given
        clusterRule.withNumberOfCoreMembers( 5 );
        clusterRule.withNumberOfEdgeMembers( 0 );

        Cluster cluster = clusterRule.startCluster();

        for ( int coreServerId = 0; coreServerId < 5; coreServerId++ )
        {
            assertEventualOverview( cluster, allOf( containsRole( Role.LEADER, 1 ), containsRole( Role.FOLLOWER, 4 ) ), coreServerId );
        }

        // when
        cluster.removeCoreMemberWithMemberId( 0 );
        cluster.removeCoreMemberWithMemberId( 1 );

        for ( int coreServerId = 2; coreServerId < 5; coreServerId++ )
        {
            // then
            assertEventualOverview( cluster, allOf( containsRole( Role.LEADER, 1 ), containsRole( Role.FOLLOWER, 2 ) ), coreServerId );
        }
    }

    private void assertEventualOverview( Cluster cluster, Matcher<List<MemberInfo>> expected, int coreServerId ) throws org.neo4j.kernel.api.exceptions.KernelException, InterruptedException
    {
        assertEventually( "should have overview", () -> clusterOverview( cluster.getCoreMemberById( coreServerId ).database() ), expected, 15, SECONDS );
    }

    private Matcher<List<MemberInfo>> containsAddress( String expectedAddress )
    {
        return new FeatureMatcher<List<MemberInfo>,Long>( equalTo( 1L ), expectedAddress, "count" )
        {
            @Override
            protected Long featureValueOf( List<MemberInfo> overview )
            {
                return overview.stream().filter( info -> info.address.equals( expectedAddress ) ).count();
            }
        };
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

    private List<MemberInfo> clusterOverview( GraphDatabaseFacade db ) throws TransactionFailureException,
            ProcedureException
    {
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        KernelTransaction transaction = kernel.newTransaction( Type.implicit, READ );
        List<MemberInfo> infos = new ArrayList<>();
        try ( Statement statement = transaction.acquireStatement() )
        {

            RawIterator<Object[],ProcedureException> itr = statement.readOperations().procedureCallRead(
                    procedureName( "dbms", "cluster", ClusterOverviewProcedure.PROCEDURE_NAME ), null );

            while ( itr.hasNext() )
            {
                Object[] row = itr.next();
                infos.add( new MemberInfo( (String) row[1], Role.valueOf( (String) row[2] ) ) );
            }
        }
        return infos;
    }

    private class MemberInfo
    {
        private final String address;
        private final Role role;

        MemberInfo( String address, Role role )
        {
            this.address = address;
            this.role = role;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }
            MemberInfo that = (MemberInfo) o;
            return Objects.equals( address, that.address ) &&
                   Objects.equals( role, that.role );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( address, role );
        }

        @Override
        public String toString()
        {
            return "MemberInfo{" +
                   "address='" + address + '\'' +
                   ", role=" + role +
                   '}';
        }
    }
}
