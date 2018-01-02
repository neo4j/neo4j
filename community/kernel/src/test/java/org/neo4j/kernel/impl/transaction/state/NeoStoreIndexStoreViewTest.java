/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;

public class NeoStoreIndexStoreViewTest
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( getClass() );

    Label label = DynamicLabel.label( "Person" );

    GraphDatabaseAPI graphDb;
    NeoStoreIndexStoreView storeView;

    int labelId;
    int propertyKeyId;

    Node alistair;
    Node stefan;
    LockService locks;
    NeoStores neoStores;
    CountsTracker counts;

    @Test
    public void shouldScanExistingNodesForALabel() throws Exception
    {
        // given
        NodeUpdateCollectingVisitor visitor = new NodeUpdateCollectingVisitor();
        @SuppressWarnings( "unchecked" )
        Visitor<NodeLabelUpdate,Exception> labelVisitor = mock( Visitor.class );
        StoreScan<Exception> storeScan =
            storeView.visitNodes( new int[] { labelId }, new int[] { propertyKeyId }, visitor, labelVisitor );

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
        StoreScan<Exception> storeScan =
                storeView.visitNodes( new int[] { labelId }, new int[] { propertyKeyId }, visitor, labelVisitor );

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
        Visitor<NodePropertyUpdate, Exception> visitor = mock( Visitor.class );
        StoreScan<Exception> storeScan = storeView
                .visitNodesWithPropertyAndLabel( new IndexDescriptor( labelId, propertyKeyId ), visitor );

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

    Map<Long, Lock> lockMocks = new HashMap<>();

    @Before
    public void before() throws KernelException
    {
        graphDb = dbRule.getGraphDatabaseAPI();

        createAlistairAndStefanNodes();
        getOrCreateIds();

        neoStores = new StoreAccess( graphDb ).getRawNeoStores();
        counts = neoStores.getCounts();
        locks = mock( LockService.class, new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                Long nodeId = (Long) invocation.getArguments()[0];
                Lock lock = lockMocks.get( nodeId );
                if ( lock == null )
                {
                    lockMocks.put( nodeId, lock = mock( Lock.class ) );
                }
                return lock;
            }
        } );
        storeView = new NeoStoreIndexStoreView( locks, neoStores );
    }

    private void createAlistairAndStefanNodes()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            alistair = graphDb.createNode( label );
            alistair.setProperty( "name", "Alistair" );
            stefan = graphDb.createNode( label );
            stefan.setProperty( "name", "Stefan" );
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

    class NodeUpdateCollectingVisitor implements Visitor<NodePropertyUpdate, Exception>
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<>();

        @Override
        public boolean visit( NodePropertyUpdate element ) throws Exception
        {
            updates.add( element );
            return false;
        }

        Set<NodePropertyUpdate> getUpdates()
        {
            return updates;
        }
    }
}
