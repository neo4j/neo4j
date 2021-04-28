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

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.configuration.Config;
import org.neo4j.function.Predicates;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.CursorContext;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.index.schema.AllEntriesTokenScanReader;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.TokenScanReader;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongResourceCollections.iterator;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class LegacyDynamicIndexStoreViewTest
{
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private final StubStorageCursors cursors = new StubStorageCursors();
    private final TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
    private final TestTokenScanConsumer tokenScanConsumer = new TestTokenScanConsumer();
    private final IntPredicate propertyKeyIdFilter = mock( IntPredicate.class );
    private final AllEntriesTokenScanReader nodeLabelRanges = mock( AllEntriesTokenScanReader.class );
    private final Config config = Config.defaults();
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

    @BeforeEach
    void setUp()
    {
        when( labelScanStore.allEntityTokenRanges( CursorContext.NULL ) ).thenReturn( nodeLabelRanges );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @Test
    void visitOnlyLabeledNodes()
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

        LegacyDynamicIndexStoreView storeView = dynamicIndexStoreView();
        StoreScan storeScan =
                storeView.visitNodes( new int[]{2, 6}, propertyKeyIdFilter, propertyScanConsumer, tokenScanConsumer,
                                      false, true, NULL, INSTANCE );
        storeScan.run( StoreScan.NO_EXTERNAL_UPDATES );

        assertThat( tokenScanConsumer.batches.size() ).isEqualTo( 1 );
        assertThat( tokenScanConsumer.batches.get( 0 ).size() ).isEqualTo( nodeIds.length );
    }

    @Test
    void propertyUpdateVisitorVisitOnlyTargetRelationships()
    {
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
            id++;
        }
        int targetPropertyKeyId = cursors.propertyKeyTokenHolder().getIdByName( targetPropertyKey );
        IntPredicate propertyKeyIdFilter = value -> value == targetPropertyKeyId;

        LegacyDynamicIndexStoreView storeView = dynamicIndexStoreView();
        StoreScan storeScan =
                storeView.visitRelationships( targetTypeArray, propertyKeyIdFilter, propertyScanConsumer, tokenScanConsumer,
                                              false, true, NULL, INSTANCE );
        storeScan.run( StoreScan.NO_EXTERNAL_UPDATES );

        // Then make sure all the fitting relationships where included for properties
        assertThat( propertyScanConsumer.batches.size() ).isEqualTo( 1 );
        assertThat( propertyScanConsumer.batches.get( 0 ).size() ).isEqualTo( wantedPropertyUpdates );
        // and that we saw all relationships in token scan since we are doing a full scan
        assertThat( tokenScanConsumer.batches.size() ).isEqualTo( 1 );
        assertThat( tokenScanConsumer.batches.get( 0 ).size() ).isEqualTo( id );
    }

    @Test
    void shouldDelegateToFullScanStoreViewForRelationships()
    {
        // Given
        FullScanStoreView fullScanStoreView = mock( FullScanStoreView.class );
        LegacyDynamicIndexStoreView dynamicIndexStoreView = dynamicIndexStoreView( fullScanStoreView );
        IntPredicate propertyKeyIdFilter = Predicates.ALWAYS_TRUE_INT;
        PageCacheTracer cacheTracer = NULL;
        int[] typeIds = {1};

        // When
        dynamicIndexStoreView.visitRelationships( typeIds, propertyKeyIdFilter, propertyScanConsumer, tokenScanConsumer, false, true,
                cacheTracer, INSTANCE );

        // Then
        Mockito.verify( fullScanStoreView, Mockito.times( 1 ) ).visitRelationships( typeIds, propertyKeyIdFilter,
                 propertyScanConsumer, tokenScanConsumer, false, true, cacheTracer, INSTANCE );
    }

    private LegacyDynamicIndexStoreView dynamicIndexStoreView()
    {
        LockService locks = LockService.NO_LOCK_SERVICE;
        Supplier<StorageReader> storageReaderSupplier = () -> cursors;
        return new LegacyDynamicIndexStoreView( new FullScanStoreView( locks, storageReaderSupplier, Config.defaults(), jobScheduler ), labelScanStore,
                locks, storageReaderSupplier, NullLogProvider.getInstance(), config );
    }

    private LegacyDynamicIndexStoreView dynamicIndexStoreView( FullScanStoreView fullScanStoreView )
    {
        LockService locks = LockService.NO_LOCK_SERVICE;
        Supplier<StorageReader> storageReaderSupplier = () -> cursors;
        return new LegacyDynamicIndexStoreView( fullScanStoreView, labelScanStore, locks, storageReaderSupplier,
                NullLogProvider.getInstance(), config );
    }
}
