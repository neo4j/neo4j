/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test.ha;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.function.IntFunction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;

/**
 * This test ensures that read-only slaves cannot make any modifications.
 */
public class ReadOnlySlaveTest
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( tx_push_factor, "2" )
            .withInstanceSetting( read_only, new IntFunction<String>()
            {
                @Override
                public String apply( int oneBasedServerId )
                {
                    return oneBasedServerId == 2 ? Settings.TRUE : null;
                }
            } );

    @Test
    public void givenClusterWithReadOnlySlaveWhenWriteTxOnSlaveThenCommitFails() throws Throwable
    {
        // When
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase readOnlySlave = cluster.getMemberByServerId( new InstanceId( 2 ) );

        try ( Transaction tx = readOnlySlave.beginTx() )
        {
            readOnlySlave.createNode();
            tx.success();
        }
        catch ( TransactionFailureException ex )
        {
            // Then
            assertThat( ex.getCause(), instanceOf( ReadOnlyDbException.class ) );
        }
    }

    @Test
    public void givenClusterWithReadOnlySlaveWhenChangePropertyOnSlaveThenThrowException() throws Throwable
    {
        // Given
        ManagedCluster cluster = clusterRule.startCluster();
        Node node;
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            node = master.createNode();
            tx.success();
        }

        // When
        HighlyAvailableGraphDatabase readOnlySlave = cluster.getMemberByServerId( new InstanceId( 2 ) );

        try ( Transaction tx = readOnlySlave.beginTx() )
        {
            Node slaveNode = readOnlySlave.getNodeById( node.getId() );


            // Then
            slaveNode.setProperty( "foo", "bar" );
            tx.success();
        }
        catch ( TransactionFailureException ex )
        {
            // Ok!
            assertThat( ex.getCause(), instanceOf( ReadOnlyDbException.class ) );
        }
    }

    @Test
    public void givenClusterWithReadOnlySlaveWhenAddNewLabelOnSlaveThenThrowException() throws Throwable
    {
        // Given
        ManagedCluster cluster = clusterRule.startCluster();
        Node node;
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            node = master.createNode();
            tx.success();
        }

        // When
        HighlyAvailableGraphDatabase readOnlySlave = cluster.getMemberByServerId( new InstanceId( 2 ) );

        try ( Transaction tx = readOnlySlave.beginTx() )
        {
            Node slaveNode = readOnlySlave.getNodeById( node.getId() );


            // Then
            slaveNode.addLabel( DynamicLabel.label( "FOO" ) );
            tx.success();
        }
        catch ( TransactionFailureException ex )
        {
            // Ok!
            assertThat( ex.getCause(), instanceOf( ReadOnlyDbException.class ) );
        }
    }

    @Test
    public void givenClusterWithReadOnlySlaveWhenAddNewRelTypeOnSlaveThenThrowException() throws Throwable
    {
        // Given
        ManagedCluster cluster = clusterRule.startCluster();
        Node node;
        Node node2;
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            node = master.createNode();
            node2 = master.createNode();
            tx.success();
        }

        // When
        HighlyAvailableGraphDatabase readOnlySlave = cluster.getMemberByServerId( new InstanceId( 2 ) );

        try ( Transaction tx = readOnlySlave.beginTx() )
        {
            Node slaveNode = readOnlySlave.getNodeById( node.getId() );
            Node slaveNode2 = readOnlySlave.getNodeById( node2.getId() );


            // Then
            slaveNode.createRelationshipTo( slaveNode2, DynamicRelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
        catch ( TransactionFailureException ex )
        {
            // Ok!
            assertThat( ex.getCause(), instanceOf( ReadOnlyDbException.class ) );
        }
    }
}
