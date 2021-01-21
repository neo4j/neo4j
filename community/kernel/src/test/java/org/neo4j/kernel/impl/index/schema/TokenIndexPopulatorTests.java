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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;

class TokenIndexPopulatorTests extends IndexPopulatorTests<TokenScanKey,TokenScanValue,Layout<TokenScanKey, TokenScanValue>>
{
    @Override
    IndexFiles createIndexFiles( FileSystemAbstraction fs, TestDirectory directory, IndexDescriptor indexDescriptor )
    {
        return new IndexFiles.SingleFile( fs, directory.homePath().resolve( DatabaseFile.LABEL_SCAN_STORE.getName() ) );
    }

    @Override
    IndexDescriptor indexDescriptor()
    {
        //Not used for token indexes yet
        return null;
    }

    @Override
    Layout<TokenScanKey,TokenScanValue> createLayout()
    {
        return new TokenScanLayout();
    }

    @Override
    byte failureByte()
    {
        return TokenIndexPopulator.FAILED;
    }

    @Override
    byte populatingByte()
    {
        return TokenIndexPopulator.POPULATING;
    }

    @Override
    byte onlineByte()
    {
        return TokenIndexPopulator.ONLINE;
    }

    @Override
    TokenIndexPopulator createPopulator( PageCache pageCache )
    {
        return createPopulator( pageCache, new Monitors(), "" );
    }

    private TokenIndexPopulator createPopulator( PageCache pageCache, Monitors monitors, String monitorTag )
    {
        return new TokenIndexPopulator( pageCache, DatabaseLayout.ofFlat( directory.homePath() ), indexFiles, fs, false, Config.defaults(),
                monitors, monitorTag, EntityType.NODE, PageCacheTracer.NULL, "Label Scan Store" );
    }

    @Test
    void addShouldApplyAllUpdatesOnce() throws Exception
    {
        // Give
        MutableLongObjectMap<long[]> entityTokens = LongObjectMaps.mutable.empty();

        populator.create();

        List<TokenIndexEntryUpdate<?>> updates = generateSomeRandomUpdates( entityTokens );
        // Add updates to populator
        populator.add( updates, NULL );

        populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
        populator.close( true, NULL );

        verifyUpdates( entityTokens );
    }

    @Test
    void updaterShouldApplyUpdates() throws Exception
    {
        // Give
        MutableLongObjectMap<long[]> entityTokens = LongObjectMaps.mutable.empty();

        populator.create();

        List<TokenIndexEntryUpdate<?>> updates = generateSomeRandomUpdates( entityTokens );

        try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor, NULL ) )
        {
            for ( TokenIndexEntryUpdate<?> update : updates )
            {
                updater.process( update );
            }
        }

        // then
        populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
        populator.close( true, NULL );
        verifyUpdates( entityTokens );
    }

    @Test
    void updaterMustThrowIfProcessAfterClose() throws Exception
    {
        // given
        populator.create();
        IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor, NULL );

        // when
        updater.close();

        IllegalStateException e = assertThrows( IllegalStateException.class,
                () -> updater.process( IndexEntryUpdate.change( random.nextInt(), null, EMPTY_LONG_ARRAY, generateRandomTokens() ) ) );
        assertThat( e ).hasMessageContaining( "Updater has been closed" );
        populator.close( true, NULL );
    }

    @Test
    void shouldHandleInterleavedRandomizedUpdates() throws IndexEntryConflictException, IOException
    {
        // Give
        int numberOfEntities = 1_000;
        long currentScanId = 0;
        MutableLongObjectMap<long[]> entityTokens = LongObjectMaps.mutable.empty();

        populator.create();

        while ( currentScanId < numberOfEntities )
        {
            // Collect a batch of max 100 updates from scan
            List<TokenIndexEntryUpdate<?>> updates = new ArrayList<>();
            for ( int i = 0; i < 100 && currentScanId < numberOfEntities; i++ )
            {
                generateRandomUpdate( currentScanId, entityTokens, updates );

                // Advance scan
                currentScanId++;
            }
            // Add updates to populator
            populator.add( updates, NULL );

            // Interleave external updates in id range lower than currentScanId
            try ( IndexUpdater updater = populator.newPopulatingUpdater( NodePropertyAccessor.EMPTY, NULL ) )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    long entityId = random.nextLong( currentScanId );
                    // Current tokens for the entity in the tree
                    long[] beforeTokens = entityTokens.get( entityId );
                    if ( beforeTokens == null )
                    {
                        beforeTokens = EMPTY_LONG_ARRAY;
                    }
                    long[] afterTokens = generateRandomTokens();
                    entityTokens.put( entityId, Arrays.copyOf( afterTokens, afterTokens.length ) );
                    updater.process( IndexEntryUpdate.change( entityId, null, beforeTokens, afterTokens ) );
                }
            }
        }

        populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
        populator.close( true, NULL );

        verifyUpdates( entityTokens );
    }

    @Test
    void shouldRelayMonitorCallsToRegisteredGBPTreeMonitorWithoutTag()
    {
        // Given
        AtomicBoolean checkpointCompletedCall = new AtomicBoolean();
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( getCheckpointCompletedListener( checkpointCompletedCall ) );
        populator = createPopulator( pageCache, monitors, "tag" );

        // When
        populator.create();
        populator.close( true, NULL );

        // Then
        assertTrue( checkpointCompletedCall.get() );
    }

    @Test
    void shouldNotRelayMonitorCallsToRegisteredGBPTreeMonitorWithDifferentTag()
    {
        // Given
        AtomicBoolean checkpointCompletedCall = new AtomicBoolean();
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( getCheckpointCompletedListener( checkpointCompletedCall ), "differentTag" );
        populator = createPopulator( pageCache, monitors, "tag" );

        // When
        populator.create();
        populator.close( true, NULL );

        // Then
        assertFalse( checkpointCompletedCall.get() );
    }

    @Test
    void shouldRelayMonitorCallsToRegisteredGBPTreeMonitorWithTag()
    {
        // Given
        AtomicBoolean checkpointCompletedCall = new AtomicBoolean();
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( getCheckpointCompletedListener( checkpointCompletedCall ), "tag" );
        populator = createPopulator( pageCache, monitors, "tag" );

        // When
        populator.create();
        populator.close( true, NULL );

        // Then
        assertTrue( checkpointCompletedCall.get() );
    }

    private GBPTree.Monitor.Adaptor getCheckpointCompletedListener( AtomicBoolean checkpointCompletedCall )
    {
        return new GBPTree.Monitor.Adaptor()
        {
            @Override
            public void checkpointCompleted()
            {
                checkpointCompletedCall.set( true );
            }
        };
    }

    private void generateRandomUpdate( long entityId, MutableLongObjectMap<long[]> trackingState, List<TokenIndexEntryUpdate<?>> updates )
    {
        long[] addTokens = generateRandomTokens();
        if ( addTokens.length != 0 )
        {
            TokenIndexEntryUpdate<?> update = IndexEntryUpdate.change( entityId, null, EMPTY_LONG_ARRAY, addTokens );
            updates.add( update );

            // Add update to tracking structure
            trackingState.put( entityId, Arrays.copyOf( addTokens, addTokens.length ) );
        }
    }

    private List<TokenIndexEntryUpdate<?>> generateSomeRandomUpdates( MutableLongObjectMap<long[]> entityTokens )
    {
        long currentScanId = 0;
        List<TokenIndexEntryUpdate<?>> updates = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            generateRandomUpdate( currentScanId, entityTokens, updates );

            // Advance scan
            currentScanId++;
        }
        return updates;
    }

    /**
     * Generate array of random tokens.
     * Generated array is empty with a certain probability.
     * Generated array contains specific tokens with different probability to get varying distribution - some bitset
     * should be quite full, while others should be quite empty and more likely to become empty with later updates.
     */
    private long[] generateRandomTokens()
    {
        long[] allTokens = new long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L};
        double[] allTokensRatio = new double[]{0.9, 0.8, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.01, 0.001};
        double emptyRatio = 0.1;

        if ( random.nextDouble() < emptyRatio )
        {
            return EMPTY_LONG_ARRAY;
        }
        else
        {
            LongArrayList longArrayList = new LongArrayList();

            for ( int i = 0; i < allTokens.length; i++ )
            {
                if ( random.nextDouble() < allTokensRatio[i] )
                {
                    longArrayList.add( allTokens[i] );
                }
            }
            return longArrayList.toArray();
        }
    }

    /**
     * Compares the state of the tree with the expected values.
     * @param expected Mapping from entity id to expected entity tokens.
     */
    private void verifyUpdates( MutableLongObjectMap<long[]> expected ) throws IOException
    {
        // Verify that everything in the tree is expected to exist.
        try ( GBPTree<TokenScanKey,TokenScanValue> tree = getTree();
              Seeker<TokenScanKey,TokenScanValue> scan = scan( tree ) )
        {
            while ( scan.next() )
            {
                TokenScanKey key = scan.key();
                TokenScanValue value = scan.value();
                long entityIdBase = key.idRange * TokenScanValue.RANGE_SIZE;

                for ( int i = 0; i < TokenScanValue.RANGE_SIZE; i++ )
                {
                    long mask = 1L << i;
                    long posInBits = value.bits & mask;
                    if ( posInBits != 0 )
                    {
                        long entity = entityIdBase + i;
                        long[] tokens = expected.remove( entity );
                        assertThat( tokens ).withFailMessage( "Entity " + entity + " contained unexpected token " + key.tokenId + " in tree" )
                                .contains( key.tokenId );

                        // Put back the rest of the tokens that we haven't verified yet
                        if ( tokens.length != 1 )
                        {
                            expected.put( entity, ArrayUtils.removeElement( tokens, key.tokenId ) );
                        }
                    }
                }
            }
        }

        // Verify that nothing expected was missing from the tree
        expected.forEachKeyValue( ( entityId, tokenIds ) ->
                assertThat( tokenIds ).withFailMessage( "Tokens " + Arrays.toString( tokenIds ) + " not found in tree for entity " + entityId ).isEmpty() );
    }

    private Seeker<TokenScanKey,TokenScanValue> scan( GBPTree<TokenScanKey,TokenScanValue> tree ) throws IOException
    {
        TokenScanKey lowest = layout.newKey();
        layout.initializeAsLowest( lowest );
        TokenScanKey highest = layout.newKey();
        layout.initializeAsHighest( highest );
        return tree.seek( lowest, highest, NULL );
    }
}
