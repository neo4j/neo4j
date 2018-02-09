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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;

/**
 * This test ensures that read-only slaves cannot make any modifications.
 */
public class ReadOnlySlaveIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withSharedSetting( tx_push_factor, "2" )
            .withInstanceSetting( read_only, oneBasedServerId -> oneBasedServerId == 2 ? Settings.TRUE : null );

    @Test
    public void givenClusterWithReadOnlySlaveWhenWriteTxOnSlaveThenCommitFails()
    {
        // When
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase readOnlySlave = cluster.getMemberByServerId( new InstanceId( 2 ) );

        try ( Transaction tx = readOnlySlave.beginTx() )
        {
            readOnlySlave.createNode();
            tx.success();
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ex )
        {
            // Then
        }
    }

    @Test
    public void givenClusterWithReadOnlySlaveWhenChangePropertyOnSlaveThenThrowException()
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
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ex )
        {
            // Ok!
        }
    }

    @Test
    public void givenClusterWithReadOnlySlaveWhenAddNewLabelOnSlaveThenThrowException()
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
            slaveNode.addLabel( Label.label( "FOO" ) );
            tx.success();
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ex )
        {
            // Ok!
        }
    }

    @Test
    public void givenClusterWithReadOnlySlaveWhenAddNewRelTypeOnSlaveThenThrowException()
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
            slaveNode.createRelationshipTo( slaveNode2, RelationshipType.withName( "KNOWS" ) );
            tx.success();
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ex )
        {
            // Ok!
        }
    }

    @Test
    public void givenClusterWithReadOnlySlaveWhenCreatingNodeOnMasterThenSlaveShouldBeAbleToPullUpdates()
    {
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Label label = Label.label( "label" );

        try ( Transaction tx = master.beginTx() )
        {
            master.createNode( label );
            tx.success();
        }

        Iterable<HighlyAvailableGraphDatabase> allMembers = cluster.getAllMembers();
        for ( HighlyAvailableGraphDatabase member : allMembers )
        {
            try ( Transaction tx = member.beginTx() )
            {
                long count = count( member.findNodes( label ) );
                tx.success();
                assertEquals( 1, count );
            }
        }
    }
}
