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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.index.StoreScan.ExternalUpdatesCheck;
import org.neo4j.kernel.impl.index.schema.TokenScanStore;
import org.neo4j.kernel.impl.index.schema.TokenScanWriter;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.batchimport.Configuration.DEFAULT;
import static org.neo4j.internal.batchimport.Configuration.withBatchSize;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.index.schema.FullStoreChangeStream.EMPTY;
import static org.neo4j.kernel.impl.index.schema.TokenScanStore.labelScanStore;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.EntityTokenUpdate.tokenChanges;

@ExtendWith( RandomExtension.class )
@PageCacheExtension
class ReadEntityIdsStepTest
{
    private static final int TOKEN_ID = 0;

    @Inject
    private RandomRule random;

    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Test
    void shouldSeeRecentUpdatesRightInFrontOfExternalUpdatesPoint() throws Exception
    {
        // given
        DatabaseLayout layout = DatabaseLayout.ofFlat( directory.homePath() );
        TokenScanStore scanStore =
                labelScanStore( pageCache, layout, directory.getFileSystem(), EMPTY, writable(), new Monitors(), immediate(), defaults(), NULL, INSTANCE );
        try ( Lifespan life = new Lifespan( scanStore ) )
        {
            long initialHighNodeId = 1_000 + random.nextInt( 100 );
            BitSet expectedEntityIds = new BitSet();
            BitSet seenEntityIds = new BitSet();
            populateScanStore( scanStore, expectedEntityIds, initialHighNodeId );
            ControlledUpdatesCheck externalUpdatesCheck = new ControlledUpdatesCheck( scanStore, expectedEntityIds );
            Configuration configuration = withBatchSize( DEFAULT, 100 );
            Stage stage = new Stage( "Test", null, configuration, 0 )
            {
                {
                    add( new ReadEntityIdsStep( control(), configuration,
                            cursorContext -> new LegacyTokenScanViewIdIterator( scanStore.newReader(), new int[]{TOKEN_ID}, CursorContext.NULL ), NULL,
                            externalUpdatesCheck, new AtomicBoolean( true ) ) );
                    add( new CollectEntityIdsStep( control(), configuration, seenEntityIds ) );
                }
            };

            // when
            stage.execute().awaitCompletion();

            // then what?
            assertThat( seenEntityIds ).isEqualTo( expectedEntityIds );
        }
    }

    private void populateScanStore( TokenScanStore scanStore, BitSet entityIds, long count ) throws IOException
    {
        try ( TokenScanWriter writer = scanStore.newWriter( CursorContext.NULL ) )
        {
            long id = 0;
            for ( int i = 0; i < count; i++ )
            {
                writer.write( added( id ) );
                entityIds.set( (int) id );
                id += random.nextInt( 1, 5 );
            }
        }
    }

    private EntityTokenUpdate added( long id )
    {
        return tokenChanges( id, EMPTY_LONG_ARRAY, new long[]{TOKEN_ID} );
    }

    private class ControlledUpdatesCheck implements ExternalUpdatesCheck
    {
        private final TokenScanStore scanStore;
        private final BitSet expectedEntityIds;

        ControlledUpdatesCheck( TokenScanStore scanStore, BitSet expectedEntityIds )
        {
            this.scanStore = scanStore;
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
            try ( TokenScanWriter writer = scanStore.newWriter( CursorContext.NULL ) )
            {
                for ( int i = 0; i < numIds; i++ )
                {
                    long candidateId = currentlyIndexedNodeId + i + 1;
                    if ( !expectedEntityIds.get( (int) candidateId ) )
                    {
                        writer.write( added( candidateId ) );
                        expectedEntityIds.set( (int) candidateId );
                    }
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
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
}
