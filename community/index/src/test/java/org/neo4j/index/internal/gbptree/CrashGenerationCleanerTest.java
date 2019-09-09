/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static org.junit.Assert.assertEquals;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.index.internal.gbptree.TreeNode.Overflow;
import static org.neo4j.index.internal.gbptree.TreeNode.setKeyCount;
import static org.neo4j.test.rule.PageCacheRule.config;

public class CrashGenerationCleanerTest
{
    private final FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( this.getClass(), fileSystemRule.get() );
    private final RandomRule randomRule = new RandomRule();
    @Rule
    public RuleChain ruleChain = RuleChain
            .outerRule( fileSystemRule ).around( testDirectory ).around( pageCacheRule ).around( randomRule );

    private static final String FILE_NAME = "index";
    private static final int PAGE_SIZE = 256;

    private PagedFile pagedFile;
    private final Layout<MutableLong,MutableLong> layout = longLayout().build();
    private final TreeNode<MutableLong,MutableLong> treeNode = new TreeNodeFixedSize<>( PAGE_SIZE, layout );
    private final ExecutorService executor = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
    private final TreeState checkpointedTreeState = new TreeState( 0, 9, 10, 0, 0, 0, 0, 0, 0, 0, true, true );
    private final TreeState unstableTreeState = new TreeState( 0, 10, 12, 0, 0, 0, 0, 0, 0, 0, true, true );
    private final List<GBPTreeCorruption.PageCorruption> possibleCorruptionsInInternal = Arrays.asList(
            GBPTreeCorruption.crashed( GBPTreePointerType.leftSibling() ),
            GBPTreeCorruption.crashed( GBPTreePointerType.rightSibling() ),
            GBPTreeCorruption.crashed( GBPTreePointerType.successor() ),
            GBPTreeCorruption.crashed( GBPTreePointerType.child( 0 ) )
    );
    private final List<GBPTreeCorruption.PageCorruption> possibleCorruptionsInLeaf = Arrays.asList(
            GBPTreeCorruption.crashed( GBPTreePointerType.leftSibling() ),
            GBPTreeCorruption.crashed( GBPTreePointerType.rightSibling() ),
            GBPTreeCorruption.crashed( GBPTreePointerType.successor() )
    );

    @Before
    public void setupPagedFile() throws IOException
    {
        PageCache pageCache = pageCacheRule
                .getPageCache( fileSystemRule.get(), config().withPageSize( PAGE_SIZE ).withAccessChecks( true ) );
        pagedFile = pageCache
                .map( testDirectory.file( FILE_NAME ), PAGE_SIZE, CREATE, DELETE_ON_CLOSE );
    }

    @After
    public void teardownPagedFile() throws IOException
    {
        pagedFile.close();
    }

    @Test
    public void shouldNotCrashOnEmptyFile() throws Exception
    {
        // GIVEN
        Page[] pages = with();
        initializeFile( pagedFile, pages );

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner( pagedFile, 0, pages.length, monitor ).clean( executor );

        // THEN
        assertPagesVisited( monitor, pages.length );
        assertCleanedCrashPointers( monitor, 0 );
    }

    @Test
    public void shouldNotReportErrorsOnCleanPages() throws Exception
    {
        // GIVEN
        Page[] pages = with(
                leafWith(),
                internalWith()
        );
        initializeFile( pagedFile, pages );

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner( pagedFile, 0, pages.length, monitor ).clean( executor );

        // THEN
        assertPagesVisited( monitor, 2 );
        assertCleanedCrashPointers( monitor, 0 );
    }

    @Test
    public void shouldCleanOneCrashPerPage() throws Exception
    {
        // GIVEN
        Page[] pages = with(
                /* left sibling */
                leafWith( GBPTreeCorruption.crashed( GBPTreePointerType.leftSibling() ) ),
                internalWith( GBPTreeCorruption.crashed( GBPTreePointerType.leftSibling() ) ),

                /* right sibling */
                leafWith( GBPTreeCorruption.crashed( GBPTreePointerType.rightSibling() ) ),
                internalWith( GBPTreeCorruption.crashed( GBPTreePointerType.rightSibling() ) ),

                /* successor */
                leafWith( GBPTreeCorruption.crashed( GBPTreePointerType.successor() ) ),
                internalWith( GBPTreeCorruption.crashed( GBPTreePointerType.successor() ) ),

                /* child */
                internalWith( GBPTreeCorruption.crashed( GBPTreePointerType.child( 0 ) ) )
        );
        initializeFile( pagedFile, pages );

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner( pagedFile, 0, pages.length, monitor ).clean( executor );

        // THEN
        assertPagesVisited( monitor, pages.length );
        assertCleanedCrashPointers( monitor, 7 );
    }

    @Test
    public void shouldCleanMultipleCrashPerPage() throws Exception
    {
        // GIVEN
        Page[] pages = with(
                leafWith(
                        GBPTreeCorruption.crashed( GBPTreePointerType.leftSibling() ),
                        GBPTreeCorruption.crashed( GBPTreePointerType.rightSibling() ),
                        GBPTreeCorruption.crashed( GBPTreePointerType.successor() ) ),
                internalWith(
                        GBPTreeCorruption.crashed( GBPTreePointerType.leftSibling() ),
                        GBPTreeCorruption.crashed( GBPTreePointerType.rightSibling() ),
                        GBPTreeCorruption.crashed( GBPTreePointerType.successor() ),
                        GBPTreeCorruption.crashed( GBPTreePointerType.child( 0 ) ) )
        );
        initializeFile( pagedFile, pages );

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner( pagedFile, 0, pages.length, monitor ).clean( executor );

        // THEN
        assertPagesVisited( monitor, pages.length );
        assertCleanedCrashPointers( monitor, 7 );
    }

    @Test
    public void shouldCleanLargeFile() throws Exception
    {
        // GIVEN
        int numberOfPages = randomRule.intBetween( 1_000, 10_000 );
        int corruptionPercent = randomRule.nextInt( 90 );
        MutableInt totalNumberOfCorruptions = new MutableInt( 0 );

        Page[] pages = new Page[numberOfPages];
        for ( int i = 0; i < numberOfPages; i++ )
        {
            Page page = randomPage( corruptionPercent, totalNumberOfCorruptions );
            pages[i] = page;
        }
        initializeFile( pagedFile, pages );

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner( pagedFile, 0, numberOfPages, monitor ).clean( executor );

        // THEN
        assertPagesVisited( monitor, numberOfPages );
        assertCleanedCrashPointers( monitor, totalNumberOfCorruptions.getValue() );
    }

    private CrashGenerationCleaner crashGenerationCleaner( PagedFile pagedFile, int lowTreeNodeId, int highTreeNodeId,
            SimpleCleanupMonitor monitor )
    {
        return new CrashGenerationCleaner( pagedFile, treeNode, lowTreeNodeId, highTreeNodeId,
                unstableTreeState.stableGeneration(), unstableTreeState.unstableGeneration(), monitor );
    }

    private void initializeFile( PagedFile pagedFile, Page... pages ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            for ( Page page : pages )
            {
                cursor.next();
                page.write( pagedFile, cursor, treeNode, layout, checkpointedTreeState, unstableTreeState );
            }
        }
    }

    /* Assertions */
    private void assertCleanedCrashPointers( SimpleCleanupMonitor monitor,
            int expectedNumberOfCleanedCrashPointers )
    {
        assertEquals( "Expected number of cleaned crash pointers to be " +
                        expectedNumberOfCleanedCrashPointers + " but was " + monitor.numberOfCleanedCrashPointers,
                expectedNumberOfCleanedCrashPointers, monitor.numberOfCleanedCrashPointers );
    }

    private void assertPagesVisited( SimpleCleanupMonitor monitor, int expectedNumberOfPagesVisited )
    {
        assertEquals( "Expected number of visited pages to be " + expectedNumberOfPagesVisited +
                        " but was " + monitor.numberOfPagesVisited,
                expectedNumberOfPagesVisited, monitor.numberOfPagesVisited );
    }

    /* Random page */
    private Page randomPage( int corruptionPercent, MutableInt totalNumberOfCorruptions )
    {
        int numberOfCorruptions = 0;
        boolean internal = randomRule.nextBoolean();
        if ( randomRule.nextInt( 100 ) < corruptionPercent )
        {
            int maxCorruptions = internal ? possibleCorruptionsInInternal.size() : possibleCorruptionsInLeaf.size();
            numberOfCorruptions = randomRule.intBetween( 1, maxCorruptions );
            totalNumberOfCorruptions.add( numberOfCorruptions );
        }
        return internal ? randomInternal( numberOfCorruptions ) : randomLeaf( numberOfCorruptions );
    }

    private Page randomLeaf( int numberOfCorruptions )
    {
        Collections.shuffle( possibleCorruptionsInLeaf );
        GBPTreeCorruption.PageCorruption[] corruptions = new GBPTreeCorruption.PageCorruption[numberOfCorruptions];
        for ( int i = 0; i < numberOfCorruptions; i++ )
        {
            corruptions[i] = possibleCorruptionsInLeaf.get( i );
        }
        return leafWith( corruptions );
    }

    private Page randomInternal( int numberOfCorruptions )
    {
        Collections.shuffle( possibleCorruptionsInInternal );
        GBPTreeCorruption.PageCorruption[] corruptions = new GBPTreeCorruption.PageCorruption[numberOfCorruptions];
        for ( int i = 0; i < numberOfCorruptions; i++ )
        {
            corruptions[i] = possibleCorruptionsInInternal.get( i );
        }
        return internalWith( corruptions );
    }

    /* Page */
    private Page[] with( Page... pages )
    {
        return pages;
    }

    private Page leafWith( GBPTreeCorruption.PageCorruption<MutableLong,MutableLong>... pageCorruptions )
    {
        return new Page( PageType.LEAF, pageCorruptions );
    }

    private Page internalWith( GBPTreeCorruption.PageCorruption<MutableLong,MutableLong>... pageCorruptions )
    {
        return new Page( PageType.INTERNAL, pageCorruptions );
    }

    private class Page
    {
        private final PageType type;
        private final GBPTreeCorruption.PageCorruption<MutableLong,MutableLong>[] pageCorruptions;

        private Page( PageType type, GBPTreeCorruption.PageCorruption<MutableLong,MutableLong>... pageCorruptions )
        {
            this.type = type;
            this.pageCorruptions = pageCorruptions;
        }

        private void write( PagedFile pagedFile, PageCursor cursor, TreeNode<MutableLong,MutableLong> node, Layout<MutableLong,MutableLong> layout,
                TreeState checkpointedTreeState, TreeState unstableTreeState ) throws IOException
        {
            type.write( cursor, node, layout, checkpointedTreeState );
            for ( GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> pc : pageCorruptions )
            {
                pc.corrupt( cursor, layout, node, unstableTreeState );
            }
        }
    }

    enum PageType
    {
        LEAF
                {
                    @Override
                    void write( PageCursor cursor, TreeNode<MutableLong,MutableLong> treeNode, Layout<MutableLong,MutableLong> layout,
                            TreeState treeState )
                    {
                        treeNode.initializeLeaf( cursor, treeState.stableGeneration(), treeState.unstableGeneration() );
                    }
                },
        INTERNAL
                {
                    @Override
                    void write( PageCursor cursor, TreeNode<MutableLong,MutableLong> treeNode, Layout<MutableLong,MutableLong> layout,
                            TreeState treeState )
                    {
                        treeNode.initializeInternal( cursor, treeState.stableGeneration(), treeState.unstableGeneration() );
                        long base = IdSpace.MIN_TREE_NODE_ID;
                        int keyCount;
                        for ( keyCount = 0; treeNode.internalOverflow( cursor, keyCount, layout.newKey() ) == Overflow.NO;
                              keyCount++ )
                        {
                            long child = base + keyCount;
                            treeNode.setChildAt( cursor, child, keyCount, treeState.stableGeneration(), treeState.unstableGeneration() );
                        }
                        setKeyCount( cursor, keyCount );
                    }
                };

        abstract void write( PageCursor cursor, TreeNode<MutableLong,MutableLong> treeNode,
                Layout<MutableLong,MutableLong> layout, TreeState treeState );
    }
}
