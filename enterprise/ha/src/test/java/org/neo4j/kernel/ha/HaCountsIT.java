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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.tooling.GlobalGraphOperations.at;

public class HaCountsIT
{
    private static final Label LABEL = DynamicLabel.label( "label" );
    private static final String PROPERTY_NAME = "prop";
    private static final String PROPERTY_VALUE = "value";

    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() );

    private ManagedCluster cluster;
    private HighlyAvailableGraphDatabase master;
    private HighlyAvailableGraphDatabase slave1;
    private HighlyAvailableGraphDatabase slave2;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
        master = cluster.getMaster();
        slave1 = cluster.getAnySlave();
        slave2 = cluster.getAnySlave( slave1 );
        clearDatabase();
    }

    private void clearDatabase() throws InterruptedException
    {
        try ( Transaction tx = master.beginTx() )
        {
            for ( IndexDefinition index : master.schema().getIndexes() )
            {
                index.drop();
            }
            tx.success();
        }

        try ( Transaction tx = master.beginTx() )
        {
            for ( Node node : at( master ).getAllNodes() )
            {
                for ( Relationship relationship : node.getRelationships() )
                {
                    relationship.delete();
                }
                node.delete();
            }
            tx.success();
        }
        cluster.sync();
    }

    @Test
    public void shouldUpdateCountsOnSlavesWhenCreatingANodeOnMaster() throws Exception
    {
        // when creating a node on the master
        createANode( master, LABEL, PROPERTY_VALUE, PROPERTY_NAME );

        // and the slaves got the updates
        cluster.sync( master );

        // then the slaves has updated counts
        assertOnNodeCounts( 1, 1, LABEL, master );
        assertOnNodeCounts( 1, 1, LABEL, slave1 );
        assertOnNodeCounts( 1, 1, LABEL, slave2 );
    }

    @Test
    public void shouldUpdateCountsOnMasterAndSlaveWhenCreatingANodeOnSlave() throws Exception
    {
        // when creating a node on the slave
        createANode( slave1, LABEL, PROPERTY_VALUE, PROPERTY_NAME );

        // and the master and slave2 got the updates
        cluster.sync( slave1 );

        // then the master and the other slave have updated counts
        assertOnNodeCounts( 1, 1, LABEL, master );
        assertOnNodeCounts( 1, 1, LABEL, slave1 );
        assertOnNodeCounts( 1, 1, LABEL, slave2 );
    }

    @Test
    public void shouldUpdateCountsOnSlavesWhenCreatingAnIndexOnMaster() throws Exception
    {
        // when creating a node on the master
        createANode( master, LABEL, PROPERTY_VALUE, PROPERTY_NAME );
        IndexDescriptor indexDescriptor = createAnIndex( master, LABEL, PROPERTY_NAME );
        awaitOnline( master, indexDescriptor );

        // and the slaves got the updates
        cluster.sync( master );

        awaitOnline( slave1, indexDescriptor );
        awaitOnline( slave2, indexDescriptor );

        // then the slaves has updated counts
        assertOnIndexCounts( 0, 1, 1, 1, indexDescriptor, master );
        assertOnIndexCounts( 0, 1, 1, 1, indexDescriptor, slave1 );
        assertOnIndexCounts( 0, 1, 1, 1, indexDescriptor, slave2 );
    }

    @Test
    public void shouldUpdateCountsOnClusterWhenCreatingANodeOnSlaveAndAnIndexOnMaster() throws Exception
    {
        // when creating a node on the master
        createANode( slave1, LABEL, PROPERTY_VALUE, PROPERTY_NAME );
        IndexDescriptor indexDescriptor = createAnIndex( master, LABEL, PROPERTY_NAME );
        awaitOnline( master, indexDescriptor );

        // and the updates are propagate in the cluster
        cluster.sync();

        awaitOnline( slave1, indexDescriptor );
        awaitOnline( slave2, indexDescriptor );

        // then the slaves has updated counts
        assertOnIndexCounts( 0, 1, 1, 1, indexDescriptor, master );
        assertOnIndexCounts( 0, 1, 1, 1, indexDescriptor, slave1 );
        assertOnIndexCounts( 0, 1, 1, 1, indexDescriptor, slave2 );
    }

    private void createANode( HighlyAvailableGraphDatabase db, Label label, String value, String property )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( property, value );
            tx.success();
        }
    }

    private IndexDescriptor createAnIndex( HighlyAvailableGraphDatabase db, Label label, String propertyName )
            throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = statement( db );
            int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( label.name() );
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( propertyName );
            IndexDescriptor index = statement.schemaWriteOperations().indexCreate( labelId, propertyKeyId );
            tx.success();
            return index;
        }
    }

    private void assertOnNodeCounts( int expectedTotalNodes, int expectedLabelledNodes,
                                     Label label, HighlyAvailableGraphDatabase db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            final Statement statement = statement( db );
            final int labelId = statement.readOperations().labelGetForName( label.name() );
            assertEquals( expectedTotalNodes, statement.readOperations().countsForNode( -1 ) );
            assertEquals( expectedLabelledNodes, statement.readOperations().countsForNode( labelId ) );
        }
    }

    private void assertOnIndexCounts( int expectedIndexUpdates, int expectedIndexSize,
                                      int expectedUniqueValues, int expectedSampleSize,
                                      IndexDescriptor indexDescriptor, HighlyAvailableGraphDatabase db )
    {
        CountsTracker counts = counts( db );
        int labelId = indexDescriptor.getLabelId();
        int propertyKeyId = indexDescriptor.getPropertyKeyId();
        assertDoubleLongEquals( expectedIndexUpdates, expectedIndexSize,
                counts.indexUpdatesAndSize( labelId, propertyKeyId, newDoubleLongRegister() ) );
        assertDoubleLongEquals( expectedUniqueValues, expectedSampleSize,
                counts.indexSample( labelId, propertyKeyId, newDoubleLongRegister() ) );
    }

    private void assertDoubleLongEquals( int expectedFirst, int expectedSecond, DoubleLongRegister actualValues )
    {
        String msg = String.format( "Expected (%d,%d) but was (%d,%d)", expectedFirst, expectedSecond,
                actualValues.readFirst(), actualValues.readSecond() );
        assertTrue( msg, actualValues.hasValues( expectedFirst, expectedSecond ) );
    }

    private CountsTracker counts( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver().resolveDependency( NeoStores.class ).getCounts();
    }

    private Statement statement( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver()
                 .resolveDependency( ThreadToStatementContextBridge.class )
                 .get();
    }

    private IndexDescriptor awaitOnline( HighlyAvailableGraphDatabase db, IndexDescriptor index )
            throws KernelException
    {
        long start = System.currentTimeMillis();
        long end = start + 3000;
        while ( System.currentTimeMillis() < end )
        {
            try ( Transaction tx = db.beginTx() )
            {
                switch ( statement( db ).readOperations().indexGetState( index ) )
                {
                case ONLINE:
                    return index;

                case FAILED:
                    throw new IllegalStateException( "Index failed instead of becoming ONLINE" );

                default:
                    break;
                }
                tx.success();

                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    // ignored
                }
            }
        }
        throw new IllegalStateException( "Index did not become ONLINE within reasonable time" );
    }
}
