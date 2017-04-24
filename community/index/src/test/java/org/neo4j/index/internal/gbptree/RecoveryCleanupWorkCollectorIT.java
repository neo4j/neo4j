/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;

import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER;

public class RecoveryCleanupWorkCollectorIT
{
    private FileSystemRule fs = new DefaultFileSystemRule();
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory( fs );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fs ).around( testDirectory ).around( pageCacheRule );

    private PageCache pageCache;
    private Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = new GroupingRecoveryCleanupWorkCollector();
    private static final LongBinaryOperator ADD = ( current, update ) -> current += update;
    private CleanLogMonitor cleanLogMonitor;
    private int totalNumberOfKeys = 100_000_000;

    @Before
    public void setUp()
    {
        pageCache = pageCacheRule.getPageCache( fs );
        cleanLogMonitor = new CleanLogMonitor();
    }

    @Test
    public void singleTree() throws Exception
    {
        doTest( 1 );
    }

    @Test
    public void multipleTrees() throws Exception
    {
        doTest( 1_000 );
    }

    private void doTest( int numberOfTrees ) throws IOException
    {
        // GIVEN
        File[] file = new File[numberOfTrees];
        long writeTime = 0;
        int keysPerTree = totalNumberOfKeys / numberOfTrees;
        for ( int i = 0; i < numberOfTrees; i++ )
        {
            file[i] = testDirectory.file( "tree" + i );
            try ( GBPTree<MutableLong,MutableLong> tree = gbpTree( file[i] ) )
            {
                long start = System.nanoTime();
                fillWithData( tree, keysPerTree );
                writeTime += System.nanoTime() - start;
                tree.checkpoint( IOLimiter.unlimited() );

                // Make dirty
                tree.writer().close();
            }
        }

        // WHEN
        GBPTree[] trees = new GBPTree[numberOfTrees];
        for ( int i = 0; i < numberOfTrees; i++ )
        {
            trees[i] = gbpTree( file[i] );
        }
        recoveryCleanupWorkCollector.run();
        IOUtils.closeAll( trees );

        // THEN
        System.out.printf( "#Trees: %d%n#Keys/tree: %d%nWrite time: %dms%nClean time: %dms%nPages visited: %d%n",
                numberOfTrees, keysPerTree,
                NANOSECONDS.toMillis( writeTime ), cleanLogMonitor.accumulatedCleanTime.get(),
                cleanLogMonitor.accumulatedNumberOfPages.get() );
    }

    private void fillWithData( GBPTree<MutableLong,MutableLong> tree, int numberOfKeys ) throws IOException
    {
        MutableLong key = new MutableLong();
        MutableLong value = new MutableLong();

        try ( Writer<MutableLong, MutableLong> writer = tree.writer() )
        {
            for ( int i = 0; i < numberOfKeys; i++ )
            {
                key.setValue( i );
                value.setValue( i );
                writer.put( key, value );
            }
        }
    }

    private GBPTree<MutableLong,MutableLong> gbpTree( File file ) throws IOException
    {
        return new GBPTree<>( pageCache, file, layout, 0, cleanLogMonitor, NO_HEADER, recoveryCleanupWorkCollector );
    }

    private class CleanLogMonitor implements GBPTree.Monitor
    {
        AtomicLong accumulatedCleanTime = new AtomicLong();
        AtomicLong accumulatedNumberOfPages = new AtomicLong();

        @Override
        public void cleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers,
                long durationMillis )
        {
            accumulatedCleanTime.accumulateAndGet( durationMillis, ADD );
            accumulatedNumberOfPages.accumulateAndGet( numberOfPagesVisited, ADD );
        }
    }
}
