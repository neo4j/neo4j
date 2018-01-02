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
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.kernel.monitoring.Monitors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.helpers.collection.Iterables.count;

/**
 * Determines when slaves should initialize a transaction on the master. This is particularly relevant for read operations
 * where we want slaves to be fast, and preferably not go to the master at all.
 */
public class WhenToInitializeTransactionOnMasterFromSlaveIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule(getClass());

    private GraphDatabaseService slave;
    private ClusterManager.ManagedCluster cluster;

    private MasterImpl.Monitor masterMonitor = mock(MasterImpl.Monitor.class);

    @Before
    public void setUp() throws Exception
    {
        cluster = clusterRule.startCluster();
        slave = cluster.getAnySlave();

        // Create some basic data
        try ( Transaction tx = slave.beginTx() )
        {
            Node node = slave.createNode( DynamicLabel.label( "Person" ) );
            node.setProperty( "name", "Bob" );
            node.createRelationshipTo( slave.createNode(), DynamicRelationshipType.withName( "KNOWS" ));

            tx.success();
        }

        // And now monitor the master for incoming calls
        cluster.getMaster().getDependencyResolver().resolveDependency( Monitors.class ).addMonitorListener( masterMonitor );
    }

    @Test
    public void shouldNotInitializeTxOnReadOnlyOpsOnNeoXaDS() throws Exception
    {
        long nodeId = 0l;

        try(Transaction transaction = slave.beginTx())
        {
            // When
            Node node = slave.getNodeById( nodeId );

            // Then
            assertDidntStartMasterTx();


            // When
            count(node.getLabels());

            // Then
            assertDidntStartMasterTx();


            // When
            readAllRels( node );

            // Then
            assertDidntStartMasterTx();


            // When
            readEachProperty(node);

            // Then
            assertDidntStartMasterTx();

            transaction.success();
        }

        // Finally
        assertDidntStartMasterTx();
    }

    private void assertDidntStartMasterTx()
    {
        verifyNoMoreInteractions( masterMonitor );
    }

    private void readAllRels( Node node )
    {
        for ( Relationship relationship : node.getRelationships() )
        {
            readEachProperty( relationship );
        }
    }

    private void readEachProperty( PropertyContainer entity )
    {
        for ( String k : entity.getPropertyKeys() )
        {
            entity.getProperty( k );
        }
    }

}
