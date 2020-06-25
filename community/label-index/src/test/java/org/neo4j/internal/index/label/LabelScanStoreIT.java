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
package org.neo4j.internal.index.label;

import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.BitSet;
import java.util.Random;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.annotations.documented.ReporterFactories.noopReporterFactory;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.collection.PrimitiveLongCollections.closingAsArray;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.ignore;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.index.label.FullStoreChangeStream.EMPTY;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@PageCacheExtension
@Neo4jLayoutExtension
@ExtendWith( {RandomExtension.class, LifeExtension.class} )
class LabelScanStoreIT
{
    @Inject
    private RandomRule random;
    @Inject
    private LifeSupport life;
    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;

    private LabelScanStore store;

    private static final int NODE_COUNT = 10_000;
    private static final int LABEL_COUNT = 12;
    private DefaultPageCacheTracer cacheTracer;

    @BeforeEach
    void before() throws IOException
    {
        cacheTracer = new DefaultPageCacheTracer();
        newLabelScanStore();
    }

    @Test
    void shouldRandomlyTestIt() throws Exception
    {
        // GIVEN
        long[] expected = new long[NODE_COUNT];
        randomModifications( expected, NODE_COUNT );

        // WHEN/THEN
        for ( int i = 0; i < 100; i++ )
        {
            verifyReads( expected );
            randomModifications( expected, NODE_COUNT / 10 );
        }
    }

    @Test
    void tracePageCacheAccessOnWrite() throws IOException
    {
        var cursorTracer = cacheTracer.createPageCursorTracer( "tracePageCacheAccessOnWrite" );
        try ( var scanWriter = store.newWriter( cursorTracer ) )
        {
            scanWriter.write( EntityTokenUpdate.tokenChanges( 0, EMPTY_LONG_ARRAY, new long[]{0, 1} ) );
        }

        assertThat( cursorTracer.pins() ).isEqualTo( 5 );
        assertThat( cursorTracer.unpins() ).isEqualTo( 5 );
        assertThat( cursorTracer.hits() ).isEqualTo( 4 );
        assertThat( cursorTracer.faults() ).isEqualTo( 1 );
    }

    @Test
    void tracePageAccessOnEmptyCheck() throws IOException
    {
        var cursorTracer = cacheTracer.createPageCursorTracer( "tracePageAccessOnEmptyCheck" );
        store.isEmpty( cursorTracer );

        assertThat( cursorTracer.pins() ).isEqualTo( 1 );
        assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
        assertThat( cursorTracer.hits() ).isEqualTo( 1 );
    }

    @Test
    void tracePageAccessOnAllEntityTokenRange() throws Exception
    {
        var cursorTracer = cacheTracer.createPageCursorTracer( "tracePageAccessOnAllEntityTokenRange" );
        try ( var scanWriter = store.newWriter( NULL ) )
        {
            scanWriter.write( EntityTokenUpdate.tokenChanges( 0, EMPTY_LONG_ARRAY, new long[]{0, 1} ) );
        }

        try ( var tokenRanges = store.allEntityTokenRanges( cursorTracer ) )
        {
            tokenRanges.forEach( range -> assertThat( range.tokens( 0 ) ).containsExactly( 0, 1 ) );
        }

        assertThat( cursorTracer.pins() ).isEqualTo( 3 );
        assertThat( cursorTracer.unpins() ).isEqualTo( 3 );
        assertThat( cursorTracer.hits() ).isEqualTo( 3 );
    }

    @Test
    void tracePageCacheAccessOnConsistencyCheck()
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( PageCursorTracer cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnConsistencyCheck" ) )
        {
            assertZeroCursor( cursorTracer );

            store.consistencyCheck( noopReporterFactory(), cursorTracer );

            assertThat( cursorTracer.pins() ).isEqualTo( 2 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 2 );
            assertThat( cursorTracer.hits() ).isEqualTo( 2 );
        }
    }

    @Test
    void tracePageCacheAccessOnNodesWithLabelRead() throws IOException
    {
        try ( var scanWriter = store.newWriter( NULL ) )
        {
            scanWriter.write( EntityTokenUpdate.tokenChanges( 0, EMPTY_LONG_ARRAY, new long[]{0, 1} ) );
        }
        var labelScanReader = store.newReader();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( PageCursorTracer cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnNdoesWithLabelRead" ) )
        {
            assertZeroCursor( cursorTracer );

            var resourceIterator = labelScanReader.entitiesWithToken( 0, cursorTracer );
            while ( resourceIterator.hasNext() )
            {
                resourceIterator.next();
            }

            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.hits() ).isOne();
        }
    }

    @Test
    void tracePageCacheAccessOnNodesWithAnyLabelRead() throws IOException
    {
        try ( var scanWriter = store.newWriter( NULL ) )
        {
            scanWriter.write( EntityTokenUpdate.tokenChanges( 0, EMPTY_LONG_ARRAY, new long[]{0, 1} ) );
        }
        var labelScanReader = store.newReader();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( PageCursorTracer cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnNodesWithAnyLabelRead" ) )
        {
            assertZeroCursor( cursorTracer );

            var resourceIterator = labelScanReader.entitiesWithAnyOfTokens( new int[]{0, 1}, cursorTracer );
            while ( resourceIterator.hasNext() )
            {
                resourceIterator.next();
            }

            assertThat( cursorTracer.pins() ).isEqualTo( 2 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 2 );
            assertThat( cursorTracer.hits() ).isEqualTo( 2 );
        }
    }

    private void assertZeroCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
        assertThat( cursorTracer.hits() ).isZero();
    }

    @Test
    void tracePageCacheAccessOnNodeLabelScan() throws IOException
    {
        try ( var scanWriter = store.newWriter( NULL ) )
        {
            scanWriter.write( EntityTokenUpdate.tokenChanges( 0, EMPTY_LONG_ARRAY, new long[]{0, 1} ) );
        }
        var labelScanReader = store.newReader();
        var pageCacheTracer = new DefaultPageCacheTracer();
        try ( PageCursorTracer cursorTracer = pageCacheTracer.createPageCursorTracer( "tracePageCacheAccessOnNodeLabelScan" ) )
        {
            assertZeroCursor( cursorTracer );

            labelScanReader.entityTokenScan( 0, cursorTracer );

            assertThat( cursorTracer.pins() ).isOne();
            assertThat( cursorTracer.unpins() ).isOne();
            assertThat( cursorTracer.hits() ).isOne();
        }
    }

    @Test
    public void shouldRemoveEmptyBitMaps() throws IOException
    {
        TokenScanLayout labelScanLayout = new TokenScanLayout();
        int nodeId = 1;
        long nodeIdRange = NativeTokenScanWriter.rangeOf( nodeId );
        int labelId = 1;
        long[] noLabels = new long[0];
        long[] singleLabel = new long[]{labelId};
        TokenScanKey key = labelScanLayout.newKey();
        key.set( labelId, nodeIdRange );

        // Add
        try ( TokenScanWriter writer = store.newWriter( NULL ) )
        {
            writer.write( EntityTokenUpdate.tokenChanges( nodeId, noLabels, singleLabel ) );
        }
        store.force( IOLimiter.UNLIMITED, NULL );
        store.shutdown();

        // Verify exists
        try ( GBPTree<TokenScanKey,TokenScanValue> tree = openReadOnlyGBPTree( labelScanLayout ) )
        {
            try ( Seeker<TokenScanKey,TokenScanValue> seek = tree.seek( key, key, NULL ) )
            {
                assertTrue( seek.next(), "Expected to find the newly inserted entry" );
            }
        }

        // Remove
        newLabelScanStore();
        try ( TokenScanWriter writer = store.newWriter( NULL ) )
        {
            writer.write( EntityTokenUpdate.tokenChanges( nodeId, singleLabel, noLabels ) );
        }
        store.force( IOLimiter.UNLIMITED, NULL );
        store.shutdown();

        // Verify don't exists
        try ( GBPTree<TokenScanKey,TokenScanValue> tree = openReadOnlyGBPTree( labelScanLayout ) )
        {
            try ( Seeker<TokenScanKey,TokenScanValue> seek = tree.seek( key, key, NULL ) )
            {
                assertFalse( seek.next(), "Expected tree to be empty after removing the last label" );
            }
        }
    }

    private GBPTree<TokenScanKey,TokenScanValue> openReadOnlyGBPTree( TokenScanLayout labelScanLayout )
    {
        return new GBPTree<>( pageCache, databaseLayout.labelScanStore(), labelScanLayout, 0, NO_MONITOR, NO_HEADER_READER,
                NO_HEADER_WRITER, ignore(), true, PageCacheTracer.NULL, Sets.immutable.empty() );
    }

    private void newLabelScanStore() throws IOException
    {
        if ( store != null )
        {
            store.shutdown();
        }

        store = life.add( TokenScanStore.labelScanStore( pageCache, databaseLayout, fileSystem, EMPTY, false, new Monitors(), immediate(), PageCacheTracer.NULL,
                EmptyMemoryTracker.INSTANCE ) );
    }

    private void verifyReads( long[] expected )
    {
        TokenScanReader reader = store.newReader();
        for ( int i = 0; i < LABEL_COUNT; i++ )
        {
            long[] actualNodes = closingAsArray( reader.entitiesWithToken( i, NULL ) );
            long[] expectedNodes = nodesWithLabel( expected, i );
            assertArrayEquals( expectedNodes, actualNodes );
        }
    }

    static long[] nodesWithLabel( long[] expected, int labelId )
    {
        int mask = 1 << labelId;
        int count = 0;
        for ( long labels : expected )
        {
            if ( (labels & mask) != 0 )
            {
                count++;
            }
        }

        long[] result = new long[count];
        int cursor = 0;
        for ( int nodeId = 0; nodeId < expected.length; nodeId++ )
        {
            long labels = expected[nodeId];
            if ( (labels & mask) != 0 )
            {
                result[cursor++] = nodeId;
            }
        }
        return result;
    }

    private void randomModifications( long[] expected, int count ) throws IOException
    {
        BitSet editedNodes = new BitSet();
        try ( TokenScanWriter writer = store.newWriter( NULL ) )
        {
            for ( int i = 0; i < count; i++ )
            {
                int nodeId = random.nextInt( NODE_COUNT );
                if ( editedNodes.get( nodeId ) )
                {
                    i--;
                    continue;
                }

                int changeSize = random.nextInt( 3 ) + 1;
                long labels = expected[nodeId];
                long[] labelsBefore = getLabels( labels );
                for ( int j = 0; j < changeSize; j++ )
                {
                    labels = flipRandom( labels, LABEL_COUNT, random.random() );
                }
                long[] labelsAfter = getLabels( labels );
                editedNodes.set( nodeId );

                EntityTokenUpdate labelChanges = EntityTokenUpdate.tokenChanges( nodeId, labelsBefore, labelsAfter );
                writer.write( labelChanges );
                expected[nodeId] = labels;
            }
        }
    }

    static long flipRandom( long existingLabels, int highLabelId, Random random )
    {
        return existingLabels ^ (1 << random.nextInt( highLabelId ));
    }

    public static long[] getLabels( long bits )
    {
        long[] result = new long[Long.bitCount( bits )];
        for ( int labelId = 0, c = 0; labelId < LABEL_COUNT; labelId++ )
        {
            int mask = 1 << labelId;
            if ( (bits & mask) != 0 )
            {
                result[c++] = labelId;
            }
        }
        return result;
    }
}
