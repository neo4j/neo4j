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
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.procedures.GetServersProcedure;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.cluster_allow_reads_on_followers;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;

@RunWith( Parameterized.class )
public class ClusterDiscoveryIT
{
    @Parameterized.Parameter( 0 )
    public String ignored; // <- JUnit is happy only if this is here!
    @Parameterized.Parameter( 1 )
    public Map<String,String> config;
    @Parameterized.Parameter( 2 )
    public boolean expectFollowersAsReadEndPoints;

    @Parameterized.Parameters( name = "{0}")
    public static Collection<Object[]> params()
    {
        return Arrays.asList(
                new Object[]{"with followers as read end points",
                        singletonMap( cluster_allow_reads_on_followers.name(), Settings.TRUE ), true},
                new Object[]{"no followers as read end points",
                        singletonMap( cluster_allow_reads_on_followers.name(), Settings.FALSE ), false}
        );
    }

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 );

    @Test
    public void shouldFindReadWriteAndRouteServers() throws Exception
    {
        // when
        Cluster cluster = clusterRule.withSharedCoreParams( config ).withNumberOfReadReplicas( 1 ).startCluster();

        // then
        int cores = cluster.coreMembers().size();
        int readReplicas = cluster.readReplicas().size();
        int readEndPoints = expectFollowersAsReadEndPoints ? (cores - 1 + readReplicas) : readReplicas;
        for ( int i = 0; i < 3; i++ )
        {
            List<Map<String,Object>> members = getMembers( cluster.getCoreMemberById( i ).database() );

            assertEquals( 1, members.stream().filter( x -> x.get( "role" ).equals( "WRITE" ) )
                    .flatMap( x -> Arrays.stream( (Object[]) x.get( "addresses" ) ) ).count() );

            assertEquals( readEndPoints, members.stream().filter( x -> x.get( "role" ).equals( "READ" ) )
                    .flatMap( x -> Arrays.stream( (Object[]) x.get( "addresses" ) ) ).count() );

            assertEquals( cores, members.stream().filter( x -> x.get( "role" ).equals( "ROUTE" ) )
                    .flatMap( x -> Arrays.stream( (Object[]) x.get( "addresses" ) ) ).count() );
        }
    }

    @Test
    public void shouldNotBeAbleToDiscoverFromReadReplicas() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withSharedCoreParams( config ).withNumberOfReadReplicas( 2 ).startCluster();

        try
        {
            // when
            getMembers( cluster.getReadReplicaById( 0 ).database() );
            fail( "Should not be able to discover members from read replicas" );
        }
        catch ( ProcedureException ex )
        {
            // then
            assertThat( ex.getMessage(), containsString( "There is no procedure with the name" ) );
        }
    }

    @SuppressWarnings( "unchecked" )
    private List<Map<String,Object>> getMembers( GraphDatabaseFacade db ) throws TransactionFailureException, org
            .neo4j.kernel.api.exceptions.ProcedureException
    {
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        KernelTransaction transaction = kernel.newTransaction( Type.implicit, AnonymousContext.read() );
        try ( Statement statement = transaction.acquireStatement() )
        {
            // when
            List<Object[]> currentMembers = asList( statement.procedureCallOperations()
                    .procedureCallRead( procedureName( "dbms", "cluster", "routing", GetServersProcedure.NAME ),
                            new Object[0] ) );

            return (List<Map<String, Object>>) currentMembers.get( 0 )[1];
        }
    }
}
