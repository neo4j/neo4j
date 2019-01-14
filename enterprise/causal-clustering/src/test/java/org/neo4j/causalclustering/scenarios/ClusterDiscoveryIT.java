/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Transaction.Type;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
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
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ProcedureNames.GET_SERVERS_V1;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;

@RunWith( Parameterized.class )
public class ClusterDiscoveryIT
{
    @Parameterized.Parameter( 0 )
    public String ignored; // <- JUnit is happy only if this is here!
    @Parameterized.Parameter( 1 )
    public Map<String,String> config;
    @Parameterized.Parameter( 2 )
    public boolean expectFollowersAsReadEndPoints;

    @Parameterized.Parameters( name = "{0}" )
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
    public final ClusterRule clusterRule = new ClusterRule().withNumberOfCoreMembers( 3 );

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
    private List<Map<String,Object>> getMembers( GraphDatabaseFacade db ) throws TransactionFailureException,
            ProcedureException
    {
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        try ( Session session = kernel.beginSession( AnonymousContext.read() );
              Transaction tx = session.beginTransaction( Type.implicit ) )
        {
            // when
            List<Object[]> currentMembers =
                    asList( tx.procedures().procedureCallRead( procedureName( GET_SERVERS_V1.fullyQualifiedProcedureName() ), new Object[0] ) );

            return (List<Map<String,Object>>) currentMembers.get( 0 )[1];
        }
    }
}
