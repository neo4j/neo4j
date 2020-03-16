/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.index.label.AllEntriesTokenScanReader;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.RelationshipTypeScanStore;
import org.neo4j.internal.index.label.TokenScanReader;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongResourceCollections.iterator;
import static org.neo4j.internal.index.label.RelationshipTypeScanStoreUtil.withRTSS;
import static org.neo4j.internal.index.label.RelationshipTypeScanStoreUtil.withoutRTSS;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

class DynamicIndexStoreViewTest
{
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private final RelationshipTypeScanStore relationshipTypeScanStore = mock( RelationshipTypeScanStore.class );
    private final StubStorageCursors cursors = new StubStorageCursors();
    private final Visitor<EntityUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );
    private final Visitor<EntityTokenUpdate,Exception> tokenUpdateVisitor = mock( Visitor.class );
    private final IntPredicate propertyKeyIdFilter = mock( IntPredicate.class );
    private final AllEntriesTokenScanReader nodeLabelRanges = mock( AllEntriesTokenScanReader.class );
    private final AllEntriesTokenScanReader relationshipTypeRanges = mock( AllEntriesTokenScanReader.class );

    @BeforeEach
    void setUp()
    {
        when( labelScanStore.allEntityTokenRanges()).thenReturn( nodeLabelRanges );
        when( relationshipTypeScanStore.allEntityTokenRanges() ).thenReturn( relationshipTypeRanges );
    }

    @Test
    void visitOnlyLabeledNodes() throws Exception
    {
        TokenScanReader labelScanReader = mock( TokenScanReader.class );
        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
        when( nodeLabelRanges.maxCount() ).thenReturn( 1L );

        long[] nodeIds = {1, 2, 3, 4, 5, 6, 7, 8};
        PrimitiveLongResourceIterator labeledNodesIterator = iterator( null, nodeIds );
        when( labelScanReader.entitiesWithAnyOfTokens( eq( new int[]{2, 6} ), any() ) ).thenReturn( labeledNodesIterator );
        for ( long nodeId : nodeIds )
        {
            cursors.withNode( nodeId ).propertyId( 1 ).relationship( 1 ).labels( 2, 6 );
        }
        // Create a couple of more nodes, just lying around
        for ( long i = 0, id = nodeIds[nodeIds.length - 1] + 1; i < 10; i++ )
        {
            cursors.withNode( id );
        }

        DynamicIndexStoreView storeView = dynamicIndexStoreView();
        StoreScan<Exception> storeScan =
                storeView.visitNodes( new int[]{2, 6}, propertyKeyIdFilter, propertyUpdateVisitor, tokenUpdateVisitor, false, NULL );
        storeScan.run();

        verify( tokenUpdateVisitor, times( nodeIds.length ) ).visit( any() );
    }

    @Test
    void propertyUpdateVisitorVisitOnlyTargetRelationships() throws Throwable
    {
        withRTSS( () ->
        {
            TokenScanReader relationshipScanReader = mock( TokenScanReader.class );
            when( relationshipTypeScanStore.newReader() ).thenReturn( relationshipScanReader );
            when( relationshipTypeRanges.maxCount() ).thenReturn( 1L );

            int targetType = 1;
            int notTargetType = 2;
            int[] targetTypeArray = {targetType};
            String targetPropertyKey = "key";
            String notTargetPropertyKey = "not-key";
            Value propertyValue = Values.stringValue( "value" );
            MutableLongList relationshipsWithTargetType = LongLists.mutable.empty();
            long id = 0;
            int wantedPropertyUpdates = 5;
            for ( int i = 0; i < wantedPropertyUpdates; i++ )
            {
                // Relationship fitting our target
                cursors.withRelationship( id, 1, targetType, 3 ).properties( targetPropertyKey, propertyValue );
                relationshipsWithTargetType.add( id++ );
                // Relationship with wrong property
                cursors.withRelationship( id, 1, targetType, 3 ).properties( notTargetPropertyKey, propertyValue );
                relationshipsWithTargetType.add( id++ );
                // Relationship with wrong type
                cursors.withRelationship( id, 1, notTargetType, 3 ).properties( targetPropertyKey, propertyValue );
            }
            PrimitiveLongResourceIterator targetRelationshipsIterator = iterator( null, relationshipsWithTargetType.toArray() );
            when( relationshipScanReader.entitiesWithAnyOfTokens( eq( targetTypeArray ), any() ) ).thenReturn( targetRelationshipsIterator );
            int targetPropertyKeyId = cursors.propertyKeyTokenHolder().getIdByName( targetPropertyKey );
            IntPredicate propertyKeyIdFilter = value -> value == targetPropertyKeyId;

            DynamicIndexStoreView storeView = dynamicIndexStoreView();
            StoreScan<Exception> storeScan =
                    storeView.visitRelationships( targetTypeArray, propertyKeyIdFilter, propertyUpdateVisitor, tokenUpdateVisitor, false, NULL );
            storeScan.run();

            // Then make sure all the fitting relationships where included
            verify( propertyUpdateVisitor, times( wantedPropertyUpdates ) ).visit( any() );
            // and that we didn't visit any more relationships than what we would get from scan store
            verify( tokenUpdateVisitor, times( relationshipsWithTargetType.size() ) ).visit( any() );
        } );
    }

    @Test
    void shouldNotDelegateToNeoStoreIndexStoreViewForRelationships() throws Throwable
    {
        withRTSS( () ->
        {
            // Given
            NeoStoreIndexStoreView neoStoreIndexStoreView = mock( NeoStoreIndexStoreView.class );
            DynamicIndexStoreView dynamicIndexStoreView = dynamicIndexStoreView( neoStoreIndexStoreView );
            IntPredicate propertyKeyIdFilter = Predicates.ALWAYS_TRUE_INT;
            Visitor<EntityUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );
            Visitor<EntityTokenUpdate,Exception> relationshipTypeUpdateVisitor = mock( Visitor.class );
            PageCursorTracer cursorTracer = NULL;
            int[] typeIds = {1};
            boolean forceStoreScan = false;
            when( relationshipTypeScanStore.isEmpty() ).thenReturn( false );

            // When
            dynamicIndexStoreView
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );

            // Then
            Mockito.verify( neoStoreIndexStoreView, Mockito.times( 0 ) )
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );
        } );
    }

    @Test
    void shouldDelegateToNeoStoreIndexStoreViewForRelationshipsIfForceStoreScan() throws Throwable
    {
        withRTSS( () ->
        {
            // Given
            NeoStoreIndexStoreView neoStoreIndexStoreView = mock( NeoStoreIndexStoreView.class );
            DynamicIndexStoreView dynamicIndexStoreView = dynamicIndexStoreView( neoStoreIndexStoreView );
            IntPredicate propertyKeyIdFilter = Predicates.ALWAYS_TRUE_INT;
            Visitor<EntityUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );
            Visitor<EntityTokenUpdate,Exception> relationshipTypeUpdateVisitor = mock( Visitor.class );
            PageCursorTracer cursorTracer = NULL;
            int[] typeIds = {1};
            when( relationshipTypeScanStore.isEmpty() ).thenReturn( false );

            // When
            boolean forceStoreScan = true;
            dynamicIndexStoreView
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );

            // Then
            Mockito.verify( neoStoreIndexStoreView, Mockito.times( 1 ) )
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );
        } );
    }

    @Test
    void shouldDelegateToNeoStoreIndexStoreViewForRelationshipsIfEmptyTypeArray() throws Throwable
    {
        withRTSS( () ->
        {
            // Given
            NeoStoreIndexStoreView neoStoreIndexStoreView = mock( NeoStoreIndexStoreView.class );
            DynamicIndexStoreView dynamicIndexStoreView = dynamicIndexStoreView( neoStoreIndexStoreView );
            IntPredicate propertyKeyIdFilter = Predicates.ALWAYS_TRUE_INT;
            Visitor<EntityUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );
            Visitor<EntityTokenUpdate,Exception> relationshipTypeUpdateVisitor = mock( Visitor.class );
            PageCursorTracer cursorTracer = NULL;
            boolean forceStoreScan = false;
            when( relationshipTypeScanStore.isEmpty() ).thenReturn( false );

            // When
            int[] typeIds = EMPTY_INT_ARRAY;
            dynamicIndexStoreView
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );

            // Then
            Mockito.verify( neoStoreIndexStoreView, Mockito.times( 1 ) )
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );
        } );
    }

    @Test
    void shouldDelegateToNeoStoreIndexStoreViewForRelationshipsIfFeatureToggleOff() throws Throwable
    {
        withoutRTSS( () ->
        {
            // Given
            NeoStoreIndexStoreView neoStoreIndexStoreView = mock( NeoStoreIndexStoreView.class );
            DynamicIndexStoreView dynamicIndexStoreView = dynamicIndexStoreView( neoStoreIndexStoreView );
            IntPredicate propertyKeyIdFilter = Predicates.ALWAYS_TRUE_INT;
            Visitor<EntityUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );
            Visitor<EntityTokenUpdate,Exception> relationshipTypeUpdateVisitor = mock( Visitor.class );
            PageCursorTracer cursorTracer = NULL;
            int[] typeIds = {1};
            boolean forceStoreScan = false;
            when( relationshipTypeScanStore.isEmpty() ).thenReturn( false );

            // When
            dynamicIndexStoreView
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );

            // Then
            Mockito.verify( neoStoreIndexStoreView, Mockito.times( 1 ) )
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );
        } );
    }

    @Test
    void shouldDelegateToNeoStoreIndexStoreViewForRelationshipsIfEmptyRTSS() throws Throwable
    {
        withRTSS( () ->
        {
            // Given
            NeoStoreIndexStoreView neoStoreIndexStoreView = mock( NeoStoreIndexStoreView.class );
            DynamicIndexStoreView dynamicIndexStoreView = dynamicIndexStoreView( neoStoreIndexStoreView );
            IntPredicate propertyKeyIdFilter = Predicates.ALWAYS_TRUE_INT;
            Visitor<EntityUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );
            Visitor<EntityTokenUpdate,Exception> relationshipTypeUpdateVisitor = mock( Visitor.class );
            PageCursorTracer cursorTracer = NULL;
            int[] typeIds = {1};
            boolean forceStoreScan = false;

            // When
            when( relationshipTypeScanStore.isEmpty() ).thenReturn( true );
            dynamicIndexStoreView
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );

            // Then
            Mockito.verify( neoStoreIndexStoreView, Mockito.times( 1 ) )
                    .visitRelationships( typeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor, forceStoreScan, cursorTracer );
        } );
    }

    private DynamicIndexStoreView dynamicIndexStoreView()
    {
        LockService locks = LockService.NO_LOCK_SERVICE;
        Supplier<StorageReader> storageReaderSupplier = () -> cursors;
        return new DynamicIndexStoreView( new NeoStoreIndexStoreView( locks, storageReaderSupplier ), labelScanStore, relationshipTypeScanStore,
                locks, storageReaderSupplier, NullLogProvider.getInstance() );
    }

    private DynamicIndexStoreView dynamicIndexStoreView( NeoStoreIndexStoreView neoStoreIndexStoreView )
    {
        LockService locks = LockService.NO_LOCK_SERVICE;
        Supplier<StorageReader> storageReaderSupplier = () -> cursors;
        return new DynamicIndexStoreView( neoStoreIndexStoreView, labelScanStore, relationshipTypeScanStore, locks, storageReaderSupplier,
                NullLogProvider.getInstance() );
    }
}
