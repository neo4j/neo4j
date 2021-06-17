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
import org.junit.jupiter.api.Test;

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexProxyProvider;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forAnyEntityTokens;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.index.schema.TokenIndexProvider.DESCRIPTOR;
import static org.neo4j.kernel.impl.locking.Locks.NO_LOCKS;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class DynamicIndexStoreViewTest
{
    private final JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();

    @AfterEach
    void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @Test
    void shouldVisitNodesUsingTokenIndex() throws Exception
    {
        long[] nodeIds = {1, 2, 3, 4, 5, 6, 7, 8};
        int[] indexedLabels = {2, 6};
        StubStorageCursors cursors = new StubStorageCursors().withTokenIndexes();
        IndexProxy indexProxy = mock( IndexProxy.class );
        IndexProxyProvider indexProxies = mock( IndexProxyProvider.class );
        StubTokenIndexReader tokenReader = new StubTokenIndexReader();
        IndexDescriptor descriptor = forSchema( forAnyEntityTokens( NODE ), DESCRIPTOR ).withName( "index" ).materialise( 0 );
        when( indexProxy.getState() ).thenReturn( InternalIndexState.ONLINE );
        when( indexProxy.newTokenReader() ).thenReturn( tokenReader );
        when( indexProxy.getDescriptor()).thenReturn( descriptor );
        when( indexProxies.getIndexProxy( any() ) ).thenReturn( indexProxy );
        // Nodes indexed by label
        for ( long nodeId : nodeIds )
        {
            cursors.withNode( nodeId ).propertyId( 1 ).relationship( 1 ).labels( 2, 6 );
            tokenReader.index( indexedLabels, nodeId );
        }

        // Nodes not indexed
        cursors.withNode( 9 ).labels( 5 );
        cursors.withNode( 10 ).labels( 6 );

        DynamicIndexStoreView storeView = dynamicIndexStoreView( cursors, indexProxies );
        TestTokenScanConsumer consumer = new TestTokenScanConsumer();
        StoreScan storeScan = storeView.visitNodes(
                indexedLabels, Predicates.ALWAYS_TRUE_INT, new TestPropertyScanConsumer(), consumer, false, true, NULL, INSTANCE );
        storeScan.run( StoreScan.NO_EXTERNAL_UPDATES );

        assertThat( consumer.batches.size() ).isEqualTo( 1 );
        assertThat( consumer.batches.get( 0 ).size() ).isEqualTo( nodeIds.length );
    }

    @Test
    void shouldVisitRelationshipsUsingTokenIndex() throws Throwable
    {
        // Given
        StubTokenIndexReader tokenReader = new StubTokenIndexReader();
        StubStorageCursors cursors = new StubStorageCursors().withTokenIndexes();
        IndexProxy indexProxy = mock( IndexProxy.class );
        IndexProxyProvider indexProxies = mock( IndexProxyProvider.class );
        IndexDescriptor descriptor = forSchema( forAnyEntityTokens( RELATIONSHIP ), DESCRIPTOR ).withName( "index" ).materialise( 0 );
        when( indexProxy.getState() ).thenReturn( InternalIndexState.ONLINE );
        when( indexProxy.getDescriptor()).thenReturn( descriptor );
        when( indexProxy.newTokenReader() ).thenReturn( tokenReader );
        when( indexProxies.getIndexProxy( any() ) ).thenReturn( indexProxy );

        int targetType = 1;
        int notTargetType = 2;
        int[] indexedTypes = {targetType};
        String targetPropertyKey = "key";
        String notTargetPropertyKey = "not-key";
        Value propertyValue = Values.stringValue( "value" );
        MutableLongList relationshipsWithTargetType = LongLists.mutable.empty();
        long id = 0;
        int wantedPropertyUpdates = 5;
        for ( int i = 0; i < wantedPropertyUpdates; i++ )
        {
            // Relationships that are indexed
            cursors.withRelationship( id, 1, targetType, 3 ).properties( targetPropertyKey, propertyValue );
            tokenReader.index( indexedTypes, id );
            relationshipsWithTargetType.add( id++ );

            // Relationship with wrong property
            cursors.withRelationship( id++, 1, targetType, 3 ).properties( notTargetPropertyKey, propertyValue );

            // Relationship with wrong type
            cursors.withRelationship( id++, 1, notTargetType, 3 ).properties( targetPropertyKey, propertyValue );
        }

        //When
        DynamicIndexStoreView storeView = dynamicIndexStoreView( cursors, indexProxies );
        TestTokenScanConsumer tokenConsumer = new TestTokenScanConsumer();
        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        StoreScan storeScan = storeView.visitRelationships(
                indexedTypes, relationType -> true, propertyScanConsumer, tokenConsumer, false, true, NULL, INSTANCE );
        storeScan.run( StoreScan.NO_EXTERNAL_UPDATES );

        // Then make sure all the fitting relationships where included
        assertThat( propertyScanConsumer.batches.size() ).isEqualTo( 1 );
        assertThat( propertyScanConsumer.batches.get( 0 ).size() ).isEqualTo( wantedPropertyUpdates );
        // and that we didn't visit any more relationships than what we would get from scan store
        assertThat( tokenConsumer.batches.size() ).isEqualTo( 1 );
        assertThat( tokenConsumer.batches.get( 0 ).size() ).isEqualTo( relationshipsWithTargetType.size() );
    }

    @Test
    void shouldVisitAllNodesWithoutTokenIndexes()
    {
        long[] nodeIds = {1, 2, 3, 4, 5, 6, 7, 8};
        int[] indexedLabels = {2, 6};
        StubStorageCursors cursors = new StubStorageCursors().withoutTokenIndexes();
        IndexProxyProvider indexProxies = mock( IndexProxyProvider.class );
        // Nodes indexed by label
        for ( long nodeId : nodeIds )
        {
            cursors.withNode( nodeId ).propertyId( 1 ).relationship( 1 ).labels( 2, 6 );
        }

        // Nodes not in index
        cursors.withNode( 9 ).labels( 5 );
        cursors.withNode( 10 ).labels( 6 );

        DynamicIndexStoreView storeView = dynamicIndexStoreView( cursors, indexProxies );
        TestTokenScanConsumer consumer = new TestTokenScanConsumer();
        StoreScan storeScan = storeView.visitNodes(
                indexedLabels, Predicates.ALWAYS_TRUE_INT, new TestPropertyScanConsumer(), consumer, false, true, NULL, INSTANCE );
        storeScan.run( StoreScan.NO_EXTERNAL_UPDATES );

        assertThat( consumer.batches.size() ).isEqualTo( 1 );
        assertThat( consumer.batches.get( 0 ).size() ).isEqualTo( nodeIds.length + 2 );
    }

    @Test
    void shouldVisitAllRelationshipsWithoutTokenIndexes()
    {
        StubStorageCursors cursors = new StubStorageCursors().withoutTokenIndexes();
        IndexProxyProvider indexProxies = mock( IndexProxyProvider.class );

        int targetType = 1;
        int notTargetType = 2;
        int[] targetTypeArray = {targetType};
        String targetPropertyKey = "key";
        Value propertyValue = Values.stringValue( "value" );
        MutableLongList relationshipsWithTargetType = LongLists.mutable.empty();
        long id = 0;
        int wantedPropertyUpdates = 5;
        for ( int i = 0; i < wantedPropertyUpdates; i++ )
        {
            // Relationship fitting our target
            cursors.withRelationship( id, 1, targetType, 3 ).properties( targetPropertyKey, propertyValue );
            relationshipsWithTargetType.add( id++ );

            // Relationship with different type
            cursors.withRelationship( id, 1, notTargetType, 3 ).properties( targetPropertyKey, propertyValue );
            relationshipsWithTargetType.add( id++ );
        }
        int targetPropertyKeyId = cursors.propertyKeyTokenHolder().getIdByName( targetPropertyKey );
        IntPredicate propertyKeyIdFilter = value -> value == targetPropertyKeyId;

        DynamicIndexStoreView storeView = dynamicIndexStoreView( cursors, indexProxies );
        TestTokenScanConsumer tokenConsumer = new TestTokenScanConsumer();
        TestPropertyScanConsumer propertyScanConsumer = new TestPropertyScanConsumer();
        StoreScan storeScan = storeView.visitRelationships(
                targetTypeArray, propertyKeyIdFilter, propertyScanConsumer, tokenConsumer, false, true, NULL, INSTANCE );
        storeScan.run( StoreScan.NO_EXTERNAL_UPDATES );

        assertThat( tokenConsumer.batches.size() ).isEqualTo( 1 );
        assertThat( tokenConsumer.batches.get( 0 ).size() ).isEqualTo( relationshipsWithTargetType.size()  );
    }

    private DynamicIndexStoreView dynamicIndexStoreView( StorageReader cursors, IndexProxyProvider indexingService )
    {
        Supplier<StorageReader> storageReaderSupplier = () -> cursors;
        return dynamicIndexStoreView( cursors, indexingService,
                new FullScanStoreView( NO_LOCK_SERVICE, storageReaderSupplier, any -> StoreCursors.NULL, Config.defaults(), jobScheduler ) );
    }

    private static DynamicIndexStoreView dynamicIndexStoreView( StorageReader cursors, IndexProxyProvider indexingService, FullScanStoreView fullScanStoreView )
    {
        Supplier<StorageReader> storageReaderSupplier = () -> cursors;
        return new DynamicIndexStoreView( fullScanStoreView, NO_LOCKS, NO_LOCK_SERVICE, Config.defaults(), indexingService, storageReaderSupplier,
                any -> StoreCursors.NULL, NullLogProvider.getInstance() );
    }
}
