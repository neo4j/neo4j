/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.index.EntityUpdates;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.emptySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class NeoStoreIndexStoreViewTest
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule();

    private final Map<Long, Lock> lockMocks = new HashMap<>();
    private final Label label = Label.label( "Person" );

    private GraphDatabaseAPI graphDb;
    private NeoStoreIndexStoreView storeView;

    private int labelId;
    private int propertyKeyId;

    private Node alistair;
    private Node stefan;
    private LockService locks;
    private NeoStores neoStores;

    @Before
    public void before() throws KernelException
    {
        graphDb = dbRule.getGraphDatabaseAPI();

        createAlistairAndStefanNodes();
        getOrCreateIds();

        neoStores = graphDb.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();

        locks = mock( LockService.class );
        when( locks.acquireNodeLock( anyLong(), any() ) ).thenAnswer(
                invocation ->
                {
                    Long nodeId = invocation.getArgument( 0 );
                    return lockMocks.computeIfAbsent( nodeId, k -> mock( Lock.class ) );
                } );
        storeView = new NeoStoreIndexStoreView( locks, neoStores );
    }

    @Test
    public void shouldScanExistingNodesForALabel() throws Exception
    {
        // given
        NodeUpdateCollectingVisitor visitor = new NodeUpdateCollectingVisitor();
        @SuppressWarnings( "unchecked" )
        Visitor<NodeLabelUpdate,Exception> labelVisitor = mock( Visitor.class );
        StoreScan<Exception> storeScan =
                storeView.visitNodes( new int[]{labelId}, id -> id == propertyKeyId, visitor, labelVisitor, false );

        // when
        storeScan.run();

        // then
        assertEquals(
            asSet(
                add( alistair.getId(), propertyKeyId, "Alistair", new long[] { labelId } ),
                add( stefan.getId(), propertyKeyId, "Stefan", new long[] { labelId } )
            ), visitor.getUpdates() );
    }

    @Test
    public void shouldIgnoreDeletedNodesDuringScan() throws Exception
    {
        // given
        deleteAlistairAndStefanNodes();

        NodeUpdateCollectingVisitor visitor = new NodeUpdateCollectingVisitor();
        @SuppressWarnings( "unchecked" )
        Visitor<NodeLabelUpdate,Exception> labelVisitor = mock( Visitor.class );
        StoreScan<Exception> storeScan =
                storeView.visitNodes( new int[]{labelId}, id -> id == propertyKeyId, visitor, labelVisitor, false );

        // when
        storeScan.run();

        // then
        assertEquals( emptySet(), visitor.getUpdates() );
    }

    @Test
    public void shouldLockNodesWhileReadingThem() throws Exception
    {
        // given
        @SuppressWarnings( "unchecked" )
        Visitor<EntityUpdates,Exception> visitor = mock( Visitor.class );
        StoreScan<Exception> storeScan =
                storeView.visitNodes( new int[]{labelId}, id -> id == propertyKeyId, visitor, null, false );

        // when
        storeScan.run();

        // then
        assertThat( "allocated locks: " + lockMocks.keySet(), lockMocks.size(), greaterThanOrEqualTo( 2 ) );
        Lock lock0 = lockMocks.get( 0L );
        Lock lock1 = lockMocks.get( 1L );
        assertNotNull( "Lock[node=0] never acquired", lock0 );
        assertNotNull( "Lock[node=1] never acquired", lock1 );
        InOrder order = inOrder( locks, lock0, lock1 );
        order.verify( locks ).acquireNodeLock( 0, LockService.LockType.READ_LOCK );
        order.verify( lock0 ).release();
        order.verify( locks ).acquireNodeLock( 1, LockService.LockType.READ_LOCK );
        order.verify( lock1 ).release();
    }

    @Test
    public void shouldReadProperties() throws EntityNotFoundException
    {
        Value value = storeView.getPropertyValue( alistair.getId(), propertyKeyId );
        assertTrue( value.equals( Values.of( "Alistair" ) ) );
    }

    @Test
    public void processAllNodeProperties() throws Exception
    {
        CopyUpdateVisitor propertyUpdateVisitor = new CopyUpdateVisitor();
        StoreViewNodeStoreScan storeViewNodeStoreScan =
                new StoreViewNodeStoreScan( neoStores.getNodeStore(), locks,
                        neoStores.getPropertyStore(), null, propertyUpdateVisitor, new int[]{labelId},
                        id -> true );

        NodeRecord nodeRecord = new NodeRecord( -1 );
        neoStores.getNodeStore().getRecord( 1L, nodeRecord, RecordLoad.FORCE );

        storeViewNodeStoreScan.process( nodeRecord );

        EntityUpdates propertyUpdates = propertyUpdateVisitor.getPropertyUpdates();
        assertNotNull( "Visitor should contain container with updates.", propertyUpdates );

        LabelSchemaDescriptor index1 = SchemaDescriptorFactory.forLabel( 0, 0 );
        LabelSchemaDescriptor index2 = SchemaDescriptorFactory.forLabel( 0, 1 );
        LabelSchemaDescriptor index3 = SchemaDescriptorFactory.forLabel( 0, 0, 1 );
        LabelSchemaDescriptor index4 = SchemaDescriptorFactory.forLabel( 1, 1 );
        List<LabelSchemaDescriptor> indexes = Arrays.asList( index1, index2, index3, index4 );

        assertThat(
                Iterables.map(
                        IndexEntryUpdate::indexKey,
                        propertyUpdates.forIndexKeys( indexes ) ),
                containsInAnyOrder( index1, index2, index3 ) );
    }

    EntityUpdates add( long nodeId, int propertyKeyId, Object value, long[] labels )
    {
        return EntityUpdates.forEntity( nodeId, labels ).added( propertyKeyId, Values.of( value ) ).build();
    }

    private void createAlistairAndStefanNodes()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            alistair = graphDb.createNode( label );
            alistair.setProperty( "name", "Alistair" );
            alistair.setProperty( "country", "UK" );
            stefan = graphDb.createNode( label );
            stefan.setProperty( "name", "Stefan" );
            stefan.setProperty( "country", "Deutschland" );
            tx.success();
        }
    }

    private void deleteAlistairAndStefanNodes()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            alistair.delete();
            stefan.delete();
            tx.success();
        }
    }

    private void getOrCreateIds() throws KernelException
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            ThreadToStatementContextBridge bridge =
                    graphDb.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );

            TokenWrite tokenWrite = bridge.getKernelTransactionBoundToThisThread( true ).tokenWrite();
            labelId = tokenWrite.labelGetOrCreateForName( "Person" );
            propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName( "name" );
            tx.success();
        }
    }

    private static class CopyUpdateVisitor implements Visitor<EntityUpdates,RuntimeException>
    {

        private EntityUpdates propertyUpdates;

        @Override
        public boolean visit( EntityUpdates element ) throws RuntimeException
        {
            propertyUpdates = element;
            return true;
        }

        public EntityUpdates getPropertyUpdates()
        {
            return propertyUpdates;
        }
    }

    class NodeUpdateCollectingVisitor implements Visitor<EntityUpdates, Exception>
    {
        private final Set<EntityUpdates> updates = new HashSet<>();

        @Override
        public boolean visit( EntityUpdates propertyUpdates )
        {
            updates.add( propertyUpdates );
            return false;
        }

        Set<EntityUpdates> getUpdates()
        {
            return updates;
        }
    }
}
