/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.Lock;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.emptySet;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.api.index.StoreScan.NO_EXTERNAL_UPDATES;
import static org.neo4j.lock.LockType.SHARED;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@DbmsExtension
class NeoStoreIndexStoreViewTest
{
    @Inject
    private GraphDatabaseAPI graphDb;
    @Inject
    private RecordStorageEngine storageEngine;
    @Inject
    private CheckPointer checkPointer;
    @Inject
    private JobScheduler jobScheduler;

    private final Map<Long, Lock> lockMocks = new HashMap<>();
    private final Label label = Label.label( "Person" );
    private final RelationshipType relationshipType = RelationshipType.withName( "Knows" );

    private NeoStoreIndexStoreView storeView;

    private int labelId;
    private int relTypeId;
    private int propertyKeyId;
    private int relPropertyKeyId;

    private Node alistair;
    private Node stefan;
    private LockService locks;
    private NeoStores neoStores;
    private Relationship aKnowsS;
    private Relationship sKnowsA;
    private StorageReader reader;
    private NodePropertyAccessor propertyAccessor;

    @BeforeEach
    void before() throws KernelException
    {
        createAlistairAndStefanNodes();
        getOrCreateIds();

        neoStores = storageEngine.testAccessNeoStores();

        locks = mock( LockService.class );
        when( locks.acquireNodeLock( anyLong(), any() ) ).thenAnswer(
                invocation ->
                {
                    Long nodeId = invocation.getArgument( 0 );
                    return lockMocks.computeIfAbsent( nodeId, k -> mock( Lock.class ) );
                } );
        when( locks.acquireRelationshipLock( anyLong(), any() ) ).thenAnswer( invocation ->
        {
            Long nodeId = invocation.getArgument( 0 );
            return lockMocks.computeIfAbsent( nodeId, k -> mock( Lock.class ) );
        } );
        storeView = new NeoStoreIndexStoreView( locks, storageEngine::newReader, Config.defaults() );
        propertyAccessor = storeView.newPropertyAccessor( PageCursorTracer.NULL, INSTANCE );
        reader = storageEngine.newReader();
    }

    @AfterEach
    void after()
    {
        propertyAccessor.close();
        reader.close();
    }

    @Test
    void shouldScanExistingNodesForALabel() throws Exception
    {
        // given
        EntityUpdateCollectingVisitor visitor = new EntityUpdateCollectingVisitor();
        @SuppressWarnings( "unchecked" )
        Visitor<List<EntityTokenUpdate>,Exception> labelVisitor = mock( Visitor.class );
        StoreScan<Exception> storeScan =
                storeView.visitNodes( new int[]{labelId}, id -> id == propertyKeyId, visitor, labelVisitor, false, true, NULL, INSTANCE );

        // when
        storeScan.run( NO_EXTERNAL_UPDATES );

        // then
        assertEquals(
            asSet(
                add( alistair.getId(), propertyKeyId, "Alistair", new long[] { labelId } ),
                add( stefan.getId(), propertyKeyId, "Stefan", new long[] { labelId } )
            ), visitor.getUpdates() );
    }

    @Test
    void shouldScanExistingRelationshipsForARelationshipType() throws Exception
    {
        // given
        EntityUpdateCollectingVisitor visitor = new EntityUpdateCollectingVisitor();
        StoreScan<Exception> storeScan =
                storeView.visitRelationships( new int[]{relTypeId}, id -> id == relPropertyKeyId, visitor, null, true, true, NULL, INSTANCE );

        // when
        storeScan.run( NO_EXTERNAL_UPDATES );

        // then
        assertEquals( asSet( add( aKnowsS.getId(), relPropertyKeyId, "long", new long[]{relTypeId} ),
                add( sKnowsA.getId(), relPropertyKeyId, "lengthy", new long[]{relTypeId} ) ), visitor.getUpdates() );
    }

    @Test
    void shouldIgnoreDeletedNodesDuringScan() throws Exception
    {
        // given
        deleteAlistairAndStefanNodes();

        EntityUpdateCollectingVisitor visitor = new EntityUpdateCollectingVisitor();
        @SuppressWarnings( "unchecked" )
        Visitor<List<EntityTokenUpdate>,Exception> labelVisitor = mock( Visitor.class );
        StoreScan<Exception> storeScan =
                storeView.visitNodes( new int[]{labelId}, id -> id == propertyKeyId, visitor, labelVisitor, false, true, NULL, INSTANCE );

        // when
        storeScan.run( NO_EXTERNAL_UPDATES );

        // then
        assertEquals( emptySet(), visitor.getUpdates() );
    }

    @Test
    void shouldIgnoreDeletedRelationshipsDuringScan() throws Exception
    {
        // given
        deleteAlistairAndStefanNodes();

        EntityUpdateCollectingVisitor visitor = new EntityUpdateCollectingVisitor();
        StoreScan<Exception> storeScan =
                storeView.visitRelationships( new int[]{relTypeId}, id -> id == relPropertyKeyId, visitor, null, true, true, NULL, INSTANCE );

        // when
        storeScan.run( NO_EXTERNAL_UPDATES );

        // then
        assertEquals( emptySet(), visitor.getUpdates() );
    }

    @Test
    void shouldLockNodesWhileReadingThem() throws Exception
    {
        // given
        @SuppressWarnings( "unchecked" )
        Visitor<List<EntityUpdates>,Exception> visitor = mock( Visitor.class );
        StoreScan<Exception> storeScan =
                storeView.visitNodes( new int[]{labelId}, id -> id == propertyKeyId, visitor, null, false, true, NULL, INSTANCE );

        // when
        storeScan.run( NO_EXTERNAL_UPDATES );

        // then
        assertThat( lockMocks.size() ).as( "allocated locks: " + lockMocks.keySet() ).isGreaterThanOrEqualTo( 2 );
        Lock lock0 = lockMocks.get( 0L );
        Lock lock1 = lockMocks.get( 1L );
        assertNotNull( lock0, "Lock[node=0] never acquired" );
        assertNotNull( lock1, "Lock[node=1] never acquired" );
        InOrder order = inOrder( locks, lock0, lock1 );
        order.verify( locks ).acquireNodeLock( 0, SHARED );
        order.verify( lock0 ).release();
        order.verify( locks ).acquireNodeLock( 1, SHARED );
        order.verify( lock1 ).release();
    }

    @Test
    void shouldLockRelationshipsWhileReadingThem() throws Exception
    {
        // given
        @SuppressWarnings( "unchecked" )
        Visitor<List<EntityUpdates>,Exception> visitor = mock( Visitor.class );
        StoreScan<Exception> storeScan = storeView.visitRelationships( new int[]{relTypeId}, id -> id == relPropertyKeyId, visitor, null,
                true, true, NULL, INSTANCE );

        // when
        storeScan.run( NO_EXTERNAL_UPDATES );

        // then
        assertThat( lockMocks.size() ).as( "allocated locks: " + lockMocks.keySet() ).isGreaterThanOrEqualTo( 2 );
        Lock lock0 = lockMocks.get( 0L );
        Lock lock1 = lockMocks.get( 1L );
        assertNotNull( lock0, "Lock[relationship=0] never acquired" );
        assertNotNull( lock1, "Lock[relationship=1] never acquired" );
        InOrder order = inOrder( locks, lock0, lock1 );
        order.verify( locks ).acquireRelationshipLock( 0, SHARED );
        order.verify( lock0 ).release();
        order.verify( locks ).acquireRelationshipLock( 1, SHARED );
        order.verify( lock1 ).release();
    }

    @Test
    void shouldReadProperties() throws EntityNotFoundException
    {
        Value value = propertyAccessor.getNodePropertyValue( alistair.getId(), propertyKeyId, PageCursorTracer.NULL );
        assertTrue( value.equals( Values.of( "Alistair" ) ) );
    }

    @Test
    void tracePageCacheAccessOnStoreViewNodeScan() throws IOException
    {
        //enforce checkpoint to flush tree caches
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "forcedCheckpoint" ) );

        var pageCacheTracer = new DefaultPageCacheTracer();
        CountingVisitor countingVisitor = new CountingVisitor();
        var scan = new NodeStoreScan<>( Config.defaults(), storageEngine.newReader(), locks, null, countingVisitor, new int[]{labelId}, id -> true, false,
                pageCacheTracer, INSTANCE );
        scan.run( NO_EXTERNAL_UPDATES );

        assertThat( countingVisitor.countedUpdates() ).isEqualTo( 2 );
        assertThat( pageCacheTracer.pins() ).isEqualTo( 4 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 4 );
        assertThat( pageCacheTracer.hits() ).isEqualTo( 4 );
    }

    @Test
    void tracePageCacheAccessOnRelationshipStoreScan() throws Exception
    {
        //enforce checkpoint to flush tree caches
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "forcedCheckpoint" ) );

        var pageCacheTracer = new DefaultPageCacheTracer();
        CountingVisitor countingVisitor = new CountingVisitor();
        var scan = new RelationshipStoreScan<>( Config.defaults(), storageEngine.newReader(), locks, null, countingVisitor, new int[]{relTypeId}, id -> true,
                false, pageCacheTracer, INSTANCE );
        scan.run( NO_EXTERNAL_UPDATES );

        assertThat( countingVisitor.countedUpdates() ).isEqualTo( 2 );
        assertThat( pageCacheTracer.pins() ).isEqualTo( 3 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 3 );
        assertThat( pageCacheTracer.hits() ).isEqualTo( 3 );
    }

    @Test
    void processAllRelationshipTypes() throws Exception
    {
        // Given
        CopyTokenUpdateVisitor<Exception> relationshipTypeUpdateVisitor = new CopyTokenUpdateVisitor<>();
        StoreScan<Exception> storeViewRelationshipStoreScan =
                storeView.visitRelationships( EMPTY_INT_ARRAY, ALWAYS_TRUE_INT, null, relationshipTypeUpdateVisitor, true, true, NULL, INSTANCE );

        // When
        storeViewRelationshipStoreScan.run( NO_EXTERNAL_UPDATES );

        // Then
        Set<EntityTokenUpdate> updates = relationshipTypeUpdateVisitor.getUpdates();
        assertThat( updates.size() ).isEqualTo( 2 );
        for ( EntityTokenUpdate update : updates )
        {
            long[] tokensAfter = update.getTokensAfter();
            assertThat( tokensAfter.length ).isEqualTo( 1 );
            assertThat( tokensAfter[0] ).isEqualTo( 0 );
            assertThat( update.getEntityId() ).satisfiesAnyOf(
                    id -> assertThat( id ).isEqualTo( 0 ),
                    id -> assertThat( id ).isEqualTo( 1 ) );
        }
    }

    private EntityUpdates add( long nodeId, int propertyKeyId, Object value, long[] labels )
    {
        return EntityUpdates.forEntity( nodeId, true ).withTokens( labels ).added( propertyKeyId, Values.of( value ) ).build();
    }

    private void createAlistairAndStefanNodes()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            alistair = tx.createNode( label );
            alistair.setProperty( "name", "Alistair" );
            alistair.setProperty( "country", "UK" );
            stefan = tx.createNode( label );
            stefan.setProperty( "name", "Stefan" );
            stefan.setProperty( "country", "Deutschland" );
            aKnowsS = alistair.createRelationshipTo( stefan, relationshipType );
            aKnowsS.setProperty( "duration", "long" );
            aKnowsS.setProperty( "irrelevant", "prop" );
            sKnowsA = stefan.createRelationshipTo( alistair, relationshipType );
            sKnowsA.setProperty( "duration", "lengthy" );
            sKnowsA.setProperty( "irrelevant", "prop" );
            tx.commit();
        }
    }

    private void deleteAlistairAndStefanNodes()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.getRelationshipById( aKnowsS.getId() ).delete();
            tx.getRelationshipById( sKnowsA.getId() ).delete();
            tx.getNodeById( alistair.getId() ).delete();
            tx.getNodeById( stefan.getId() ).delete();
            tx.commit();
        }
    }

    private void getOrCreateIds() throws KernelException
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            TokenWrite tokenWrite = ((InternalTransaction) tx ).kernelTransaction().tokenWrite();
            labelId = tokenWrite.labelGetOrCreateForName( "Person" );
            relTypeId = tokenWrite.relationshipTypeGetOrCreateForName( "Knows" );
            propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName( "name" );
            relPropertyKeyId = tokenWrite.propertyKeyGetOrCreateForName( "duration" );
            tx.commit();
        }
    }

    private static class CountingVisitor implements Visitor<List<EntityUpdates>,RuntimeException>
    {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public boolean visit( List<EntityUpdates> element ) throws RuntimeException
        {
            counter.addAndGet( element.size() );
            return true;
        }

        int countedUpdates()
        {
            return counter.get();
        }
    }

    private static class CopyUpdateVisitor implements Visitor<List<EntityUpdates>,RuntimeException>
    {
        private EntityUpdates propertyUpdates;

        @Override
        public boolean visit( List<EntityUpdates> element ) throws RuntimeException
        {
            propertyUpdates = element.get( 0 );
            return true;
        }

        EntityUpdates getPropertyUpdates()
        {
            return propertyUpdates;
        }
    }

    static class EntityUpdateCollectingVisitor implements Visitor<List<EntityUpdates>,Exception>
    {
        private final Set<EntityUpdates> updates = new HashSet<>();

        @Override
        public boolean visit( List<EntityUpdates> propertyUpdates )
        {
            updates.addAll( propertyUpdates );
            return false;
        }

        Set<EntityUpdates> getUpdates()
        {
            return updates;
        }
    }

    private static class CopyTokenUpdateVisitor<EXCEPTION extends Exception> implements Visitor<List<EntityTokenUpdate>,EXCEPTION>
    {
        private final Set<EntityTokenUpdate> updates = new HashSet<>();

        @Override
        public boolean visit( List<EntityTokenUpdate> element )
        {
            updates.addAll( element );
            return false;
        }

        Set<EntityTokenUpdate> getUpdates()
        {
            return updates;
        }
    }
}
