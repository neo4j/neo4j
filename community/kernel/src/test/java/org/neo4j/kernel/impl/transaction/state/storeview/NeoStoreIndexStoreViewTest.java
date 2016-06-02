/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.StoreViewNodeStoreScan;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.emptySetOf;

public class NeoStoreIndexStoreViewTest
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( getClass() );

    private Map<Long, Lock> lockMocks = new HashMap<>();
    private Label label = Label.label( "Person" );

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

        locks = mock( LockService.class, (Answer) invocation -> {
            Long nodeId = (Long) invocation.getArguments()[0];
            Lock lock = lockMocks.get( nodeId );
            if ( lock == null )
            {
                lockMocks.put( nodeId, lock = mock( Lock.class ) );
            }
            return lock;
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
            storeView.visitNodes( new int[] {labelId}, (id) -> id == propertyKeyId, visitor, labelVisitor );

        // when
        storeScan.run();

        // then
        assertEquals(
            asSet(
                NodePropertyUpdate.add( alistair.getId(), propertyKeyId, "Alistair", new long[] { labelId } ),
                NodePropertyUpdate.add( stefan.getId(), propertyKeyId, "Stefan", new long[] { labelId } )
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
        StoreScan<Exception> storeScan = storeView.visitNodes( new int[]{labelId}, (id) -> id == propertyKeyId,
                visitor, labelVisitor );

        // when
        storeScan.run();

        // then
        assertEquals( emptySetOf( NodePropertyUpdate.class ), visitor.getUpdates() );
    }

    @Test
    public void shouldLockNodesWhileReadingThem() throws Exception
    {
        // given
        @SuppressWarnings("unchecked")
        Visitor<NodePropertyUpdates, Exception> visitor = mock( Visitor.class );
        StoreScan<Exception> storeScan = storeView.visitNodes( new int[] {labelId}, (id) -> id == propertyKeyId,
                visitor, null );

        // when
        storeScan.run();

        // then
        assertEquals( "allocated locks: " + lockMocks.keySet(), 2, lockMocks.size() );
        Lock lock0 = lockMocks.get( 0L );
        Lock lock1 = lockMocks.get( 1L );
        assertNotNull( "Lock[node=0] never acquired", lock0 );
        assertNotNull( "Lock[node=1] never acquired", lock1 );
        InOrder order = inOrder( locks, lock0, lock1 );
        order.verify( locks ).acquireNodeLock( 0, LockService.LockType.READ_LOCK );
        order.verify( lock0 ).release();
        order.verify( locks ).acquireNodeLock( 1, LockService.LockType.READ_LOCK );
        order.verify( lock1 ).release();
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldReadProperties() throws PropertyNotFoundException, EntityNotFoundException
    {
        Property property = storeView.getProperty( alistair.getId(), propertyKeyId );
        assertTrue( property.valueEquals( "Alistair" ) );
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

        NodePropertyUpdates propertyUpdates = propertyUpdateVisitor.getPropertyUpdates();
        assertNotNull( "Visitor should containts container with 2 updates.", propertyUpdates );
        assertThat( "Node should have 2 property and we should get 2 updates",
                propertyUpdates.getPropertyUpdates(), Matchers.hasSize(2) );
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

            try ( Statement statement = bridge.get() )
            {
                labelId = statement.dataWriteOperations().labelGetOrCreateForName( "Person" );
                propertyKeyId = statement.dataWriteOperations().propertyKeyGetOrCreateForName( "name" );
            }
            tx.success();
        }
    }

    private static class CopyUpdateVisitor implements Visitor<NodePropertyUpdates,RuntimeException>
    {

        private NodePropertyUpdates propertyUpdates;

        @Override
        public boolean visit( NodePropertyUpdates element ) throws RuntimeException
        {
            propertyUpdates = new NodePropertyUpdates();
            propertyUpdates.initForNodeId( element.getNodeId() );
            propertyUpdates.addAll(element.getPropertyUpdates());
            return true;
        }

        public NodePropertyUpdates getPropertyUpdates()
        {
            return propertyUpdates;
        }
    }

    class NodeUpdateCollectingVisitor implements Visitor<NodePropertyUpdates, Exception>
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<>();

        @Override
        public boolean visit( NodePropertyUpdates propertyUpdates ) throws Exception
        {
            updates.addAll( propertyUpdates.getPropertyUpdates() );
            return false;
        }

        Set<NodePropertyUpdate> getUpdates()
        {
            return updates;
        }
    }
}
