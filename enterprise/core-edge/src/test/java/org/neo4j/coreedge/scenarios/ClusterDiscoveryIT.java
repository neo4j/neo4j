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

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.procedures.AcquireEndpointsProcedure;
import org.neo4j.coreedge.discovery.procedures.DiscoverEndpointAcquisitionServersProcedure;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.coreedge.ClusterRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.security.AccessMode.Static.READ;

public class ClusterDiscoveryIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 );

    @Test
    public void shouldDiscoverCoreClusterMembers() throws Exception
    {
        // when
        Cluster cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();

        // then

        List<Object[]> currentMembers;
        for ( int i = 0; i < 3; i++ )
        {
            currentMembers = discoverClusterMembers( cluster.getCoreMemberById( i ).database() );
            assertThat( currentMembers, containsInAnyOrder(
                    new Object[]{"127.0.0.1:8000"},
                    new Object[]{"127.0.0.1:8001"},
                    new Object[]{"127.0.0.1:8002"} ) );
        }
    }

    @Test
    public void shouldFindReadAndWriteServers() throws Exception
    {
        // when
        Cluster cluster = clusterRule.withNumberOfEdgeMembers( 1 ).startCluster();

        // then

        List<Object[]> currentMembers;
        for ( int i = 0; i < 3; i++ )
        {
            currentMembers = endPoints( cluster.getCoreMemberById( i ).database() );

            assertEquals(1, currentMembers.stream().filter( x -> x[1].equals( "WRITE" ) ).count());
            assertEquals(1, currentMembers.stream().filter( x -> x[1].equals( "READ" ) ).count());
        }
    }

    @Test
    public void shouldNotBeAbleToDiscoverFromEdgeMembers() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfEdgeMembers( 2 ).startCluster();

        try
        {
            // when
            discoverClusterMembers( cluster.getEdgeMemberById( 0 ).database() );
            fail( "Should not be able to discover members from edge members" );
        }
        catch ( ProcedureException ex )
        {
            // then
            assertThat( ex.getMessage(), containsString( "There is no procedure with the name" ) );
        }
    }

    private List<Object[]> discoverClusterMembers( GraphDatabaseFacade db ) throws TransactionFailureException, org
            .neo4j.kernel.api.exceptions.ProcedureException
    {
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        KernelTransaction transaction = kernel.newTransaction( Type.implicit, READ );
        try ( Statement statement = transaction.acquireStatement() )
        {
            // when
            return asList( statement.readOperations().procedureCallRead(
                    procedureName( "dbms", "cluster", DiscoverEndpointAcquisitionServersProcedure.NAME ),
                    new Object[0] ) );
        }
    }

    private List<Object[]> endPoints( GraphDatabaseFacade db ) throws TransactionFailureException, org
            .neo4j.kernel.api.exceptions.ProcedureException
    {
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        KernelTransaction transaction = kernel.newTransaction( Type.implicit, READ );
        try ( Statement statement = transaction.acquireStatement() )
        {
            // when
            return asList( statement.readOperations().procedureCallRead(
                    procedureName( "dbms", "cluster", AcquireEndpointsProcedure.NAME ),
                    new Object[0] ) );
        }
    }
}
