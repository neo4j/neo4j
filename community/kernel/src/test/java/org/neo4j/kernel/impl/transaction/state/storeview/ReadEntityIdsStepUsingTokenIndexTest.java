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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.schema.AnyTokenSchemaDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.StoreScan.ExternalUpdatesCheck;
import org.neo4j.kernel.impl.index.schema.DatabaseIndexContext;
import org.neo4j.kernel.impl.index.schema.IndexFiles;
import org.neo4j.kernel.impl.index.schema.TokenIndexAccessor;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.internal.batchimport.Configuration.withBatchSize;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes;

@ExtendWith( RandomExtension.class )
@PageCacheExtension
@Neo4jLayoutExtension
class ReadEntityIdsStepUsingTokenIndexTest
{
    private static final int TOKEN_ID = 0;
    private static final AnyTokenSchemaDescriptor SCHEMA_DESCRIPTOR = SchemaDescriptor.forAnyEntityTokens( EntityType.NODE );
    private static final IndexDescriptor INDEX_DESCRIPTOR = IndexPrototype.forSchema( SCHEMA_DESCRIPTOR ).withName( "index" ).materialise( 1 );

    @Inject
    TestDirectory testDir;

    @Inject
    PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private RandomRule random;

    @Test
    void shouldSeeRecentUpdatesRightInFrontOfExternalUpdatesPoint() throws Exception
    {
        // given
        long entityCount = 1_000 + random.nextInt( 100 );
        BitSet expectedEntityIds = new BitSet();
        BitSet seenEntityIds = new BitSet();

        try ( var indexAccessor = indexAccessor() )
        {
            populateTokenIndex( indexAccessor, expectedEntityIds, entityCount );
            Configuration configuration = withBatchSize( DEFAULT, 100 );
            Stage stage = new Stage( "Test", null, configuration, 0 )
            {
                {
                    add( new ReadEntityIdsStep( control(),
                                                configuration,
                                                cursorContext -> new TokenIndexScanIdIterator(
                                                        indexAccessor.newTokenReader(), new int[]{TOKEN_ID}, CursorContext.NULL ),
                                                NULL,
                                                new ControlledUpdatesCheck( indexAccessor, expectedEntityIds ),
                                                new AtomicBoolean( true ) ) );
                    add( new CollectEntityIdsStep( control(), configuration, seenEntityIds ) );
                }
            };

            // when
            stage.execute().awaitCompletion();

            // then
            assertThat( seenEntityIds ).isEqualTo( expectedEntityIds );
        }
    }

    private void populateTokenIndex( TokenIndexAccessor indexAccessor, BitSet entityIds, long count ) throws Exception
    {
        try ( IndexUpdater updater = indexAccessor.newUpdater( ONLINE, CursorContext.NULL ) )
        {
            long id = 0;
            for ( int i = 0; i < count; i++ )
            {
                updater.process( IndexEntryUpdate.change( id, INDEX_DESCRIPTOR, EMPTY_LONG_ARRAY, new long[]{TOKEN_ID} ) );
                entityIds.set( (int) id );
                id += random.nextInt( 1, 5 );
            }
        }
    }

    private class ControlledUpdatesCheck implements ExternalUpdatesCheck
    {
        private final TokenIndexAccessor indexAccessor;
        private final BitSet expectedEntityIds;

        ControlledUpdatesCheck( TokenIndexAccessor indexAccessor, BitSet expectedEntityIds )
        {
            this.indexAccessor = indexAccessor;
            this.expectedEntityIds = expectedEntityIds;
        }

        @Override
        public boolean needToApplyExternalUpdates()
        {
            return random.nextBoolean();
        }

        @Override
        public void applyExternalUpdates( long currentlyIndexedNodeId )
        {
            // Apply some changes right in front of this point
            int numIds = random.nextInt( 5, 50 );
            try ( IndexUpdater updater = indexAccessor.newUpdater( ONLINE, CursorContext.NULL ) )
            {
                for ( int i = 0; i < numIds; i++ )
                {
                    long candidateId = currentlyIndexedNodeId + i + 1;
                    if ( !expectedEntityIds.get( (int) candidateId ) )
                    {
                        updater.process( IndexEntryUpdate.change( candidateId, INDEX_DESCRIPTOR, EMPTY_LONG_ARRAY, new long[]{TOKEN_ID} ) );
                        expectedEntityIds.set( (int) candidateId );
                    }
                }
            }
            catch ( IndexEntryConflictException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private static class CollectEntityIdsStep extends ProcessorStep<long[]>
    {
        private final BitSet seenEntityIds;

        CollectEntityIdsStep( StageControl control, Configuration config, BitSet seenEntityIds )
        {
            super( control, "Collector", config, 1, NULL );
            this.seenEntityIds = seenEntityIds;
        }

        @Override
        protected void process( long[] entityIds, BatchSender sender, CursorContext cursorContext ) throws Throwable
        {
            for ( long entityId : entityIds )
            {
                seenEntityIds.set( (int) entityId );
            }
        }
    }

    private TokenIndexAccessor indexAccessor()
    {
        return new TokenIndexAccessor( DatabaseIndexContext.builder( pageCache, testDir.getFileSystem(), DEFAULT_DATABASE_NAME ).build(),
                                       databaseLayout,
                                       new IndexFiles.SingleFile( testDir.getFileSystem(), databaseLayout.labelScanStore() ),
                                       config(), INDEX_DESCRIPTOR, immediate() );
    }

    private Config config()
    {
        return Config.newBuilder()
                     .set( neo4j_home, testDir.absolutePath() )
                     .set( preallocate_logical_logs, false )
                     .set( enable_scan_stores_as_token_indexes, true )
                     .build();
    }
}
