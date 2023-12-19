/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class HaCountsIT
{
    private static final Label LABEL = Label.label( "label" );
    private static final String PROPERTY_NAME = "prop";
    private static final String PROPERTY_VALUE = "value";

    @Rule
    public ClusterRule clusterRule = new ClusterRule();

    private ManagedCluster cluster;
    private HighlyAvailableGraphDatabase master;
    private HighlyAvailableGraphDatabase slave1;
    private HighlyAvailableGraphDatabase slave2;

    @Before
    public void setup()
    {
        cluster = clusterRule.startCluster();
        master = cluster.getMaster();
        slave1 = cluster.getAnySlave();
        slave2 = cluster.getAnySlave( slave1 );
        clearDatabase();
    }

    private void clearDatabase()
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
            for ( Node node : master.getAllNodes() )
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
    public void shouldUpdateCountsOnSlavesWhenCreatingANodeOnMaster()
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
    public void shouldUpdateCountsOnMasterAndSlaveWhenCreatingANodeOnSlave()
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
        SchemaIndexDescriptor schemaIndexDescriptor = createAnIndex( master, LABEL, PROPERTY_NAME );
        long indexId = awaitOnline( master, schemaIndexDescriptor );

        // and the slaves got the updates
        cluster.sync( master );

        long index1 = awaitOnline( slave1, schemaIndexDescriptor );
        long index2 = awaitOnline( slave2, schemaIndexDescriptor );

        // then the slaves has updated counts
        assertOnIndexCounts( 0, 1, 1, 1, indexId, master );
        assertOnIndexCounts( 0, 1, 1, 1, index1, slave1 );
        assertOnIndexCounts( 0, 1, 1, 1, index2, slave2 );
    }

    @Test
    public void shouldUpdateCountsOnClusterWhenCreatingANodeOnSlaveAndAnIndexOnMaster() throws Exception
    {
        // when creating a node on the master
        createANode( slave1, LABEL, PROPERTY_VALUE, PROPERTY_NAME );
        SchemaIndexDescriptor schemaIndexDescriptor = createAnIndex( master, LABEL, PROPERTY_NAME );
        long indexId = awaitOnline( master, schemaIndexDescriptor );

        // and the updates are propagate in the cluster
        cluster.sync();

        long index1 = awaitOnline( slave1, schemaIndexDescriptor );
        long index2 = awaitOnline( slave2, schemaIndexDescriptor );

        // then the slaves has updated counts
        assertOnIndexCounts( 0, 1, 1, 1, indexId, master );
        assertOnIndexCounts( 0, 1, 1, 1, index1, slave1 );
        assertOnIndexCounts( 0, 1, 1, 1, index2, slave2 );
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

    private SchemaIndexDescriptor createAnIndex( HighlyAvailableGraphDatabase db, Label label, String propertyName )
            throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( db );
            int labelId = ktx.tokenWrite().labelGetOrCreateForName( label.name() );
            int propertyKeyId = ktx.tokenWrite().propertyKeyGetOrCreateForName( propertyName );
            IndexReference index = ktx.schemaWrite()
                                                   .indexCreate( SchemaDescriptorFactory.forLabel( labelId, propertyKeyId ), null );
            tx.success();
            return DefaultIndexReference.toDescriptor( index );
        }
    }

    private void assertOnNodeCounts( int expectedTotalNodes, int expectedLabelledNodes,
                                     Label label, HighlyAvailableGraphDatabase db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            KernelTransaction transaction = kernelTransaction( db );
            final int labelId = transaction.tokenRead().nodeLabel( label.name() );
            assertEquals( expectedTotalNodes, transaction.dataRead().countsForNode( -1 ) );
            assertEquals( expectedLabelledNodes, transaction.dataRead().countsForNode( labelId ) );
        }
    }

    private void assertOnIndexCounts( int expectedIndexUpdates, int expectedIndexSize,
                                      int expectedUniqueValues, int expectedSampleSize,
                                      long indexId, HighlyAvailableGraphDatabase db )
    {
        CountsTracker counts = counts( db );
        assertDoubleLongEquals( expectedIndexUpdates, expectedIndexSize,
                counts.indexUpdatesAndSize( indexId, newDoubleLongRegister() ) );
        assertDoubleLongEquals( expectedUniqueValues, expectedSampleSize,
                counts.indexSample( indexId, newDoubleLongRegister() ) );
    }

    private void assertDoubleLongEquals( int expectedFirst, int expectedSecond, DoubleLongRegister actualValues )
    {
        String msg = String.format( "Expected (%d,%d) but was (%d,%d)", expectedFirst, expectedSecond,
                actualValues.readFirst(), actualValues.readSecond() );
        assertTrue( msg, actualValues.hasValues( expectedFirst, expectedSecond ) );
    }

    private CountsTracker counts( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().getCounts();
    }

    private Statement statement( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver()
                 .resolveDependency( ThreadToStatementContextBridge.class )
                 .get();
    }

    private KernelTransaction kernelTransaction( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread(true);
    }

    private IndexingService indexingService( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver().resolveDependency( IndexingService.class );
    }

    private long awaitOnline( HighlyAvailableGraphDatabase db, SchemaIndexDescriptor index )
            throws KernelException
    {
        long start = System.currentTimeMillis();
        long end = start + 60_000;
        while ( System.currentTimeMillis() < end )
        {
            try ( Transaction tx = db.beginTx() )
            {
                KernelTransaction transaction = kernelTransaction( db );
                switch ( transaction.schemaRead().indexGetState( DefaultIndexReference.fromDescriptor( index  ) ) )
                {
                case ONLINE:
                    return indexingService( db ).getIndexId( index.schema() );

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
