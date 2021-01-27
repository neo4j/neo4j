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
package org.neo4j.internal.batchimport;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.RelationshipTypeScanStore;
import org.neo4j.internal.index.label.TokenScanStore;
import org.neo4j.internal.index.label.TokenScanWriter;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.batchimport.Configuration.withBatchSize;
import static org.neo4j.internal.batchimport.ProcessorAssignmentStrategies.eagerRandomSaturation;
import static org.neo4j.internal.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.store.IdUpdateListener.IGNORE;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralPageCacheExtension
class CountsStagesTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;

    private final int numEntities = 10_000;
    private final int highTokenId = 1;
    private final long[] nodeLabelIds = new long[]{0};
    private final Configuration config = withBatchSize( Configuration.DEFAULT, 10 );
    private final SimpleProgressReporter progressReporter = new SimpleProgressReporter();
    private final SimpleCountsUpdater countsUpdater = new SimpleCountsUpdater();

    @Test
    void shouldTrackNodeCountsStageProgress()
    {
        shouldTrackProgress(
                stores -> createNodes( stores.getNodeStore(), numEntities ),
                ( stores, cache ) -> new NodeCountsStage( config, cache, stores.getNodeStore(), highTokenId, countsUpdater, progressReporter, NULL ) );
    }

    @Test
    void shouldTrackRelationshipCountsStageProgress()
    {
        shouldTrackProgress(
                stores -> createRelationships( stores, numEntities ),
                ( stores, cache ) ->
                {
                    prepareCache( stores, cache );
                    return new RelationshipCountsStage( config, cache, stores.getRelationshipStore(), highTokenId, highTokenId, countsUpdater,
                                                        NumberArrayFactories.HEAP, progressReporter, NULL, INSTANCE );
                } );
    }

    @Test
    void shouldTrackNodeCountsAndLabelIndexStageProgress()
    {
        shouldTrackProgress(
                stores -> createNodes( stores.getNodeStore(), numEntities ),
                ( stores, cache ) -> new NodeCountsAndLabelIndexBuildStage( config, cache, stores.getNodeStore(), highTokenId, countsUpdater, progressReporter,
                        mockedTokenScanStore( mock( LabelScanStore.class )), NULL ) );
    }

    @Test
    void shouldTrackRelationshipCountsAndTypeIndexStageProgress()
    {
        shouldTrackProgress(
                stores -> createRelationships( stores, numEntities ),
                ( stores, cache ) ->
                {
                    prepareCache( stores, cache );
                    return new RelationshipCountsAndTypeIndexBuildStage( config, cache, stores.getRelationshipStore(), highTokenId, highTokenId, countsUpdater,
                            NumberArrayFactories.HEAP, progressReporter, mockedTokenScanStore( mock( RelationshipTypeScanStore.class ) ), NULL, INSTANCE );
                } );
    }

    private void shouldTrackProgress( Consumer<NeoStores> dataCreator, StageFactory stageFactory )
    {
        // given
        DatabaseLayout layout = DatabaseLayout.ofFlat( directory.directory( "store" ) );
        try ( NeoStores stores = new StoreFactory( layout, Config.defaults(), new DefaultIdGeneratorFactory( directory.getFileSystem(), immediate() ),
                pageCache, directory.getFileSystem(), NullLogProvider.nullLogProvider(), NULL ).openNeoStores( true,
                StoreType.NODE_LABEL, StoreType.NODE, StoreType.RELATIONSHIP ) )
        {
            dataCreator.accept( stores );

            // when
            NodeStore nodeStore = stores.getNodeStore();
            try ( NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactories.HEAP, nodeStore.getHighId(), highTokenId, INSTANCE ) )
            {
                superviseDynamicExecution( eagerRandomSaturation( config.maxNumberOfProcessors() ), stageFactory.apply( stores, cache ) );
            }

            // then
            assertThat( progressReporter.progress ).isEqualTo( numEntities );
            assertThat( countsUpdater.count.longValue() ).isEqualTo( numEntities );
        }
    }

    private void prepareCache( NeoStores stores, NodeLabelsCache cache )
    {
        for ( long nodeId = 0; nodeId < stores.getNodeStore().getHighId(); nodeId++ )
        {
            cache.put( nodeId, nodeLabelIds );
        }
    }

    private <T extends TokenScanStore> T mockedTokenScanStore( T tokenScanStore )
    {
        when( tokenScanStore.newBulkAppendWriter( any() ) ).thenReturn( TokenScanWriter.EMPTY_WRITER );
        return tokenScanStore;
    }

    private long[] createNodes( NodeStore nodeStore, int count )
    {
        NodeRecord record = nodeStore.newRecord();
        long[] ids = new long[count];
        for ( int i = 0; i < count; i++ )
        {
            record.setId( nodeStore.nextId( PageCursorTracer.NULL ) );
            record.initialize( true, NULL_REFERENCE.longValue(), false, NULL_REFERENCE.longValue(), NO_LABELS_FIELD.longValue() );
            NodeLabelsField.parseLabelsField( record ).put( nodeLabelIds, nodeStore, nodeStore.getDynamicLabelStore(), PageCursorTracer.NULL, INSTANCE );
            nodeStore.updateRecord( record, IGNORE, PageCursorTracer.NULL );
            ids[i] = record.getId();
        }
        return ids;
    }

    private void createRelationships( NeoStores stores, int numEntities )
    {
        // We still need to create some nodes so that the node store gets correct highId
        int numNodes = 2;
        NodeStore nodeStore = stores.getNodeStore();
        long[] nodes = createNodes( nodeStore, numNodes );
        RelationshipStore relationshipStore = stores.getRelationshipStore();
        RelationshipRecord record = relationshipStore.newRecord();
        for ( int i = 0; i < numEntities; i++ )
        {
            record.setId( relationshipStore.nextId( PageCursorTracer.NULL ) );
            record.initialize( true, NULL_REFERENCE.longValue(), nodes[i % nodes.length], nodes[(i + 1) % nodes.length], 0,
                    NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(), true, true );
            relationshipStore.updateRecord( record, IGNORE, PageCursorTracer.NULL );
        }
    }

    private static class SimpleCountsUpdater implements CountsAccessor.Updater
    {
        private final AtomicLong count = new AtomicLong();

        @Override
        public void incrementNodeCount( long labelId, long delta )
        {
            if ( labelId == -1 /*means any node, really*/ )
            {
                count.addAndGet( delta );
            }
        }

        @Override
        public void incrementRelationshipCount( long startLabelId, int typeId, long endLabelId, long delta )
        {
            if ( startLabelId == -1 && typeId == -1 && endLabelId == -1 )
            {
                count.addAndGet( delta );
            }
        }

        @Override
        public void close()
        {
        }
    }

    private static class SimpleProgressReporter implements ProgressReporter
    {
        private long progress;

        @Override
        public void start( long max )
        {
        }

        @Override
        public void progress( long add )
        {
            this.progress += add;
        }

        @Override
        public void completed()
        {
        }
    }

    private interface StageFactory
    {
        Stage apply( NeoStores stores, NodeLabelsCache cache );
    }
}
