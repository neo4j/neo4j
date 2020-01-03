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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier.EMPTY;

@ExtendWith( TestDirectoryExtension.class )
class GBPTreeSingleWriterTest
{
    @Inject
    TestDirectory directory;
    private PageCache pageCache;
    private SimpleLongLayout layout;
    private ThreadPoolJobScheduler jobScheduler;

    @BeforeEach
    void createPageCache()
    {
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory();
        factory.open( new DefaultFileSystemAbstraction(), Configuration.EMPTY );
        MemoryAllocator mman = MemoryAllocator.createAllocator( "8 MiB", new LocalMemoryTracker() );
        jobScheduler = new ThreadPoolJobScheduler();
        pageCache = new MuninnPageCache( factory, mman, 256, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, EMPTY, jobScheduler );
        layout = SimpleLongLayout.longLayout()
                .withFixedSize( true )
                .build();
    }

    @AfterEach
    void tearDownPageCache()
    {
        pageCache.close();
        jobScheduler.close();
    }

    @Test
    void shouldReInitializeTreeLogicWithSameSplitRatioAsInitiallySet0() throws IOException
    {
        TreeHeightTracker treeHeightTracker = new TreeHeightTracker();
        try ( GBPTree<MutableLong,MutableLong> gbpTree = new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout )
                .with( treeHeightTracker )
                .build();
              Writer<MutableLong,MutableLong> writer = gbpTree.writer( 0 ) )
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
            gbpTree.visit( keyCountingVisitor );
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
              Writer<MutableLong,MutableLong> writer = gbpTree.writer( 1 ) )
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
            gbpTree.visit( keyCountingVisitor );
            for ( Integer rightmostKeyCount : keyCountingVisitor.keyCountOnRightmostPerLevel )
            {
                assertTrue( rightmostKeyCount == 0 || rightmostKeyCount == 1 );
            }
        }
    }

    private class KeyCountingVisitor extends GBPTreeVisitor.Adaptor<MutableLong,MutableLong>
    {
        private boolean newLevel;
        private List<Integer> keyCountOnLeftmostPerLevel = new ArrayList<>();
        private List<Integer> keyCountOnRightmostPerLevel = new ArrayList<>();
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

    private class TreeHeightTracker extends GBPTree.Monitor.Adaptor
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
