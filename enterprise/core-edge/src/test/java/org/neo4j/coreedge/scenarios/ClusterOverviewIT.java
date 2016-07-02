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

import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.core.ClusterOverviewProcedure;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.coreedge.ClusterRule;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.security.AccessMode.Static.READ;

public class ClusterOverviewIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() ).withNumberOfCoreServers( 3 );

    @Test
    public void shouldDiscoverCoreClusterMembers() throws Exception
    {
        // when
        Cluster cluster = clusterRule.withNumberOfEdgeServers( 1 ).startCluster();

        // then
        List<Object[]> overview;
        for ( int i = 0; i < 3; i++ )
        {
            overview = clusterOverview( cluster.getCoreServerById( i ).database() );

            assertThat( overview, containsRole( "leader", 1 ) );
            assertThat( overview, containsRole( "follower", 2 ) );
            assertThat( overview, containsRole( "read_replica", 1 ) );

            // core
            assertThat( overview, containsAddress( "127.0.0.1:8000" ) );
            assertThat( overview, containsAddress( "127.0.0.1:8001" ) );
            assertThat( overview, containsAddress( "127.0.0.1:8002" ) );

            // read replicas
            assertThat( overview, containsAddress( "127.0.0.1:9000" ) );
        }
    }

    private Matcher<? super List<Object[]>> containsAddress(String address)
    {
        return new TypeSafeMatcher<List<Object[]>>()
        {
            @Override
            public boolean matchesSafely( List<Object[]> overview )
            {
                for ( Object[] row : overview )
                {
                    if ( row[1].toString().equals( address ) )
                    {
                        return true;
                    }
                }

                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Expected to find leader in the cluster but didn't" );
            }
        };
    }

    private Matcher<? super List<Object[]>> containsRole(String role, int expectedRoleCount)
    {
        return new TypeSafeMatcher<List<Object[]>>()
        {
            @Override
            public boolean matchesSafely( List<Object[]> overview )
            {
                int numberOfMachinesForRole = 0;

                for ( Object[] row : overview )
                {
                    if ( row[2].toString().equals( role ) )
                    {
                        numberOfMachinesForRole++;
                    }
                }

                return numberOfMachinesForRole == expectedRoleCount;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Expected to find " + role + " in the cluster but didn't" );
            }
        };
    }

    private List<Object[]> clusterOverview( GraphDatabaseFacade db ) throws TransactionFailureException,
            ProcedureException
    {
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        KernelTransaction transaction = kernel.newTransaction( Type.implicit, READ );
        Statement statement = transaction.acquireStatement();

        // when
        return asList( statement.readOperations().procedureCallRead(
                procedureName( "dbms", "cluster", ClusterOverviewProcedure.NAME ),
                new Object[0] ) );
    }
}
