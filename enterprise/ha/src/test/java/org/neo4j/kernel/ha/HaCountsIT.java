/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class HaCountsIT
{
    private static final Label LABEL = Label.label( "label" );
    private static final String PROPERTY_NAME = "prop";
    private static final String PROPERTY_VALUE = "value";

    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

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
        IndexDescriptor schemaIndexDescriptor = createAnIndex( master, LABEL, PROPERTY_NAME );
        awaitOnline( master );

        // and the slaves got the updates
        cluster.sync( master );

        awaitOnline( slave1 );
        awaitOnline( slave2 );

        // then the slaves has updated counts
        assertOnIndexCounts( 0, 1, 1, 1, schemaIndexDescriptor, master );
        assertOnIndexCounts( 0, 1, 1, 1, schemaIndexDescriptor, slave1 );
        assertOnIndexCounts( 0, 1, 1, 1, schemaIndexDescriptor, slave2 );
    }

    @Test
    public void shouldUpdateCountsOnClusterWhenCreatingANodeOnSlaveAndAnIndexOnMaster() throws Exception
    {
        // when creating a node on the master
        createANode( slave1, LABEL, PROPERTY_VALUE, PROPERTY_NAME );
        IndexDescriptor schemaIndexDescriptor = createAnIndex( master, LABEL, PROPERTY_NAME );
        awaitOnline( master );

        // and the updates are propagate in the cluster
        cluster.sync();

        awaitOnline( slave1 );
        awaitOnline( slave2 );

        // then the slaves has updated counts
        assertOnIndexCounts( 0, 1, 1, 1, schemaIndexDescriptor, master );
        assertOnIndexCounts( 0, 1, 1, 1, schemaIndexDescriptor, slave1 );
        assertOnIndexCounts( 0, 1, 1, 1, schemaIndexDescriptor, slave2 );
    }

    private static void createANode( HighlyAvailableGraphDatabase db, Label label, String value, String property )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( property, value );
            tx.success();
        }
    }

    private static IndexDescriptor createAnIndex( HighlyAvailableGraphDatabase db, Label label, String propertyName )
            throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( db );
            int labelId = ktx.tokenWrite().labelGetOrCreateForName( label.name() );
            int propertyKeyId = ktx.tokenWrite().propertyKeyGetOrCreateForName( propertyName );
            IndexReference index = ktx.schemaWrite()
                                                   .indexCreate( SchemaDescriptorFactory.forLabel( labelId, propertyKeyId ) );
            tx.success();
            return (IndexDescriptor) index;
        }
    }

    private static void assertOnNodeCounts( int expectedTotalNodes, int expectedLabelledNodes, Label label, HighlyAvailableGraphDatabase db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            KernelTransaction transaction = kernelTransaction( db );
            final int labelId = transaction.tokenRead().nodeLabel( label.name() );
            assertEquals( expectedTotalNodes, transaction.dataRead().countsForNode( -1 ) );
            assertEquals( expectedLabelledNodes, transaction.dataRead().countsForNode( labelId ) );
        }
    }

    private static void assertOnIndexCounts( int expectedIndexUpdates, int expectedIndexSize, int expectedUniqueValues, int expectedSampleSize,
            IndexDescriptor indexDescriptor, HighlyAvailableGraphDatabase db )
            throws TransactionFailureException, IndexNotFoundKernelException
    {
        try ( org.neo4j.internal.kernel.api.Transaction tx = db.getDependencyResolver().resolveDependency( Kernel.class )
                .beginTransaction( explicit, AUTH_DISABLED ) )
        {
            IndexReference indexReference = tx.schemaRead().index( indexDescriptor.schema() );
            assertDoubleLongEquals( expectedIndexUpdates, expectedIndexSize, tx.schemaRead().indexUpdatesAndSize( indexReference, newDoubleLongRegister() ) );
            assertDoubleLongEquals( expectedUniqueValues, expectedSampleSize, tx.schemaRead().indexSample( indexReference, newDoubleLongRegister() ) );
        }
    }

    private static void assertDoubleLongEquals( int expectedFirst, int expectedSecond, DoubleLongRegister actualValues )
    {
        String msg = String.format( "Expected (%d,%d) but was (%d,%d)", expectedFirst, expectedSecond,
                actualValues.readFirst(), actualValues.readSecond() );
        assertTrue( msg, actualValues.hasValues( expectedFirst, expectedSecond ) );
    }

    private static KernelTransaction kernelTransaction( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread(true);
    }

    private static void awaitOnline( HighlyAvailableGraphDatabase db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 60, TimeUnit.SECONDS );
            tx.success();
        }
    }
}
