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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.PageCacheConfig;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@EphemeralTestDirectoryExtension
class GBPTreeSingleWriterTest
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension( PageCacheConfig.config().withPageSize( 512 ) );
    @Inject
    private TestDirectory directory;
    @Inject
    private PageCache pageCache;
    private SimpleLongLayout layout = SimpleLongLayout.longLayout().withFixedSize( true ).build();

    @Test
    void shouldReInitializeTreeLogicWithSameSplitRatioAsInitiallySet0() throws IOException
    {
        TreeHeightTracker treeHeightTracker = new TreeHeightTracker();
        try ( GBPTree<MutableLong,MutableLong> gbpTree = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout )
                .with( treeHeightTracker )
                .build();
              Writer<MutableLong,MutableLong> writer = gbpTree.writer( 0, NULL ) )
        {
            MutableLong dontCare = layout.value( 0 );

            long keySeed = 10_000;
            while ( treeHeightTracker.treeHeight < 5 )
            {
                MutableLong key = layout.key( keySeed-- );
                writer.put( key, dontCare );
            }
            // We now have a tree with height 6.
            // The leftmost node on all levels should have only a single key.
            KeyCountingVisitor keyCountingVisitor = new KeyCountingVisitor();
            gbpTree.visit( keyCountingVisitor, NULL );
            for ( Integer leftmostKeyCount : keyCountingVisitor.keyCountOnLeftmostPerLevel )
            {
                assertEquals( 1, leftmostKeyCount.intValue() );
            }
        }
    }

    @Test
    void shouldReInitializeTreeLogicWithSameSplitRatioAsInitiallySet1() throws IOException
    {
        TreeHeightTracker treeHeightTracker = new TreeHeightTracker();
        try ( GBPTree<MutableLong,MutableLong> gbpTree = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout )
                .with( treeHeightTracker )
                .build();
              Writer<MutableLong,MutableLong> writer = gbpTree.writer( 1, NULL ) )
        {
            MutableLong dontCare = layout.value( 0 );

            long keySeed = 0;
            while ( treeHeightTracker.treeHeight < 5 )
            {
                MutableLong key = layout.key( keySeed++ );
                writer.put( key, dontCare );
            }
            // We now have a tree with height 6.
            // The rightmost node on all levels should have either one or zero key (zero for internal nodes).
            KeyCountingVisitor keyCountingVisitor = new KeyCountingVisitor();
            gbpTree.visit( keyCountingVisitor, NULL );
            for ( Integer rightmostKeyCount : keyCountingVisitor.keyCountOnRightmostPerLevel )
            {
                assertTrue( rightmostKeyCount == 0 || rightmostKeyCount == 1 );
            }
        }
    }

    @Test
    void trackPageCacheAccessOnMerge() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "trackPageCacheAccessOnMerge" );

        assertZeroCursor( cursorTracer );

        try ( var gbpTree = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
              var treeWriter = gbpTree.writer( 0, cursorTracer ) )
        {
            treeWriter.merge( new MutableLong( 0 ), new MutableLong( 1 ), ValueMergers.overwrite() );

            assertThat( cursorTracer.pins() ).isEqualTo( 5 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 4 );
            assertThat( cursorTracer.hits() ).isEqualTo( 4 );
            assertThat( cursorTracer.faults() ).isEqualTo( 1 );
        }
    }

    @Test
    void trackPageCacheAccessOnPut() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "trackPageCacheAccessOnPut" );

        assertZeroCursor( cursorTracer );

        try ( var gbpTree = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
              var treeWriter = gbpTree.writer( 0, cursorTracer ) )
        {
            treeWriter.put( new MutableLong( 0 ), new MutableLong( 1 ) );

            assertThat( cursorTracer.pins() ).isEqualTo( 5 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 4 );
            assertThat( cursorTracer.hits() ).isEqualTo( 4 );
            assertThat( cursorTracer.faults() ).isEqualTo( 1 );
        }
    }

    @Test
    void trackPageCacheAccessOnRemove() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "trackPageCacheAccessOnRemove" );

        try ( var gbpTree = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
              var treeWriter = gbpTree.writer( 0, cursorTracer ) )
        {
            treeWriter.put( new MutableLong( 0 ), new MutableLong( 0 ) );
            assertThat( cursorTracer.pins() ).isEqualTo( 5 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 4 );
            assertThat( cursorTracer.hits() ).isEqualTo( 4 );
            assertThat( cursorTracer.faults() ).isEqualTo( 1 );

            cursorTracer.reportEvents();
            assertZeroCursor( cursorTracer );

            // we are on the same page and we do not expect any cursor events to be registered
            treeWriter.remove( new MutableLong( 0 ) );

            assertZeroCursor( cursorTracer );
        }
    }

    @Test
    void trackPageCacheAccessOnRemoveWhenNothingToRemove() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "trackPageCacheAccessOnRemoveWhenNothingToRemove" );

        assertZeroCursor( cursorTracer );

        try ( var gbpTree = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
              var treeWriter = gbpTree.writer( 0, cursorTracer ) )
        {
            treeWriter.remove( new MutableLong( 0 ) );

            assertThat( cursorTracer.pins() ).isEqualTo( 1 );
            assertThat( cursorTracer.hits() ).isEqualTo( 1 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 0 );
            assertThat( cursorTracer.faults() ).isEqualTo( 0 );
        }
    }

    @Test
    void trackPageCacheAccessOnClose() throws IOException
    {
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorTracer = pageCacheTracer.createPageCursorTracer( "trackPageCacheAccessOnClose" );

        assertZeroCursor( cursorTracer );

        try ( var gbpTree = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
              var treeWriter = gbpTree.writer( 0, cursorTracer ) )
        {
            // empty, we check that closing everything register unpins event
        }

        assertThat( cursorTracer.pins() ).isEqualTo( 1 );
        assertThat( cursorTracer.hits() ).isEqualTo( 1 );
        assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
        assertThat( cursorTracer.faults() ).isEqualTo( 0 );
    }

    private void assertZeroCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
        assertThat( cursorTracer.hits() ).isZero();
        assertThat( cursorTracer.faults() ).isZero();
    }

    private static class KeyCountingVisitor extends GBPTreeVisitor.Adaptor<MutableLong,MutableLong>
    {
        private boolean newLevel;
        private final List<Integer> keyCountOnLeftmostPerLevel = new ArrayList<>();
        private final List<Integer> keyCountOnRightmostPerLevel = new ArrayList<>();
        private int rightmostKeyCountOnLevelSoFar;

        @Override
        public void beginLevel( int level )
        {
            newLevel = true;
            rightmostKeyCountOnLevelSoFar = -1;
        }

        @Override
        public void endLevel( int level )
        {
            keyCountOnRightmostPerLevel.add( rightmostKeyCountOnLevelSoFar );
        }

        @Override
        public void beginNode( long pageId, boolean isLeaf, long generation, int keyCount )
        {
            if ( newLevel )
            {
                newLevel = false;
                keyCountOnLeftmostPerLevel.add( keyCount );
            }
            rightmostKeyCountOnLevelSoFar = keyCount;
        }
    }

    private static class TreeHeightTracker extends GBPTree.Monitor.Adaptor
    {
        int treeHeight;

        @Override
        public void treeGrowth()
        {
            treeHeight++;
        }

        @Override
        public void treeShrink()
        {
            treeHeight--;
        }
    }
}
