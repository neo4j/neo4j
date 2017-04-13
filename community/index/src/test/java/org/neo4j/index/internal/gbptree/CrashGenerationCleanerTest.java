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
import static org.neo4j.test.rule.PageCacheRule.config;

public class CrashGenerationCleanerTest
{
    private FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory( this.getClass(), fileSystemRule.get() );
    private RandomRule randomRule = new RandomRule();
    @Rule
    public RuleChain ruleChain = RuleChain
            .outerRule( fileSystemRule ).around( testDirectory ).around( pageCacheRule ).around( randomRule );

    private static final String FILE_NAME = "index";
    private static final int PAGE_SIZE = 256;

    private PagedFile pagedFile;
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final CorruptableTreeNode corruptableTreeNode = new CorruptableTreeNode( PAGE_SIZE, layout );
    private final int oldStableGeneration = 9;
    private final int stableGeneration = 10;
    private final int unstableGeneration = 12;
    private final int crashGeneration = 11;
    private final int firstChildPos = 0;
    private final int middleChildPos = corruptableTreeNode.internalMaxKeyCount() / 2;
    private final int lastChildPos = corruptableTreeNode.internalMaxKeyCount();
    private final List<PageCorruption> possibleCorruptionsInInternal = Arrays.asList(
            crashed( leftSibling() ),
            crashed( rightSibling() ),
            crashed( successor() ),
            crashed( child( firstChildPos ) ),
            crashed( child( middleChildPos ) ),
            crashed( child( lastChildPos ) )
    );
    private final List<PageCorruption> possibleCorruptionsInLeaf = Arrays.asList(
            crashed( leftSibling() ),
            crashed( rightSibling() ),
            crashed( successor() )
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
        crashGenerationCleaner( pagedFile, 0, pages.length, monitor ).clean();

        // THEN
        assertPagesVisisted( monitor, pages.length );
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
        crashGenerationCleaner( pagedFile, 0, pages.length, monitor ).clean();

        // THEN
        assertPagesVisisted( monitor, 2 );
        assertCleanedCrashPointers( monitor, 0 );
    }

    @Test
    public void shouldCleanOneCrashPerPage() throws Exception
    {
        // GIVEN
        Page[] pages = with(
                /* left sibling */
                leafWith( crashed( leftSibling() ) ),
                internalWith( crashed( leftSibling() ) ),

                /* right sibling */
                leafWith( crashed( rightSibling() ) ),
                internalWith( crashed( rightSibling() ) ),

                /* successor */
                leafWith( crashed( successor() ) ),
                internalWith( crashed( successor() ) ),

                /* child */
                internalWith( crashed( child( firstChildPos ) ) ),
                internalWith( crashed( child( middleChildPos ) ) ),
                internalWith( crashed( child( lastChildPos ) ) )
        );
        initializeFile( pagedFile, pages );

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner( pagedFile, 0, pages.length, monitor ).clean();

        // THEN
        assertPagesVisisted( monitor, pages.length );
        assertCleanedCrashPointers( monitor, 9 );
    }

    @Test
    public void shouldCleanMultipleCrashPerPage() throws Exception
    {
        // GIVEN
        Page[] pages = with(
                leafWith(
                        crashed( leftSibling() ),
                        crashed( rightSibling() ),
                        crashed( successor() ) ),
                internalWith(
                        crashed( leftSibling() ),
                        crashed( rightSibling() ),
                        crashed( successor() ),
                        crashed( child( firstChildPos ) ),
                        crashed( child( middleChildPos ) ),
                        crashed( child( lastChildPos ) ) )
        );
        initializeFile( pagedFile, pages );

        // WHEN
        SimpleCleanupMonitor monitor = new SimpleCleanupMonitor();
        crashGenerationCleaner( pagedFile, 0, pages.length, monitor ).clean();

        // THEN
        assertPagesVisisted( monitor, pages.length );
        assertCleanedCrashPointers( monitor, 9 );
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
        crashGenerationCleaner( pagedFile, 0, numberOfPages, monitor ).clean();

        // THEN
        assertPagesVisisted( monitor, numberOfPages );
        assertCleanedCrashPointers( monitor, totalNumberOfCorruptions.getValue() );
    }

    private CrashGenerationCleaner crashGenerationCleaner( PagedFile pagedFile, int lowTreeNodeId, int highTreeNodeId,
            SimpleCleanupMonitor monitor )
    {
        return new CrashGenerationCleaner( pagedFile, corruptableTreeNode, lowTreeNodeId, highTreeNodeId,
                stableGeneration, unstableGeneration, monitor );
    }

    private void initializeFile( PagedFile pagedFile, Page... pages ) throws IOException
    {
        try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            for ( Page page : pages )
            {
                cursor.next();
                page.write( cursor, corruptableTreeNode, stableGeneration, unstableGeneration, crashGeneration );
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

    private void assertPagesVisisted( SimpleCleanupMonitor monitor, int expectedNumberOfPagesVisited )
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
        PageCorruption[] corruptions = new PageCorruption[numberOfCorruptions];
        for ( int i = 0; i < numberOfCorruptions; i++ )
        {
            corruptions[i] = possibleCorruptionsInLeaf.get( i );
        }
        return leafWith( corruptions );
    }

    private Page randomInternal( int numberOfCorruptions )
    {
        Collections.shuffle( possibleCorruptionsInInternal );
        PageCorruption[] corruptions = new PageCorruption[numberOfCorruptions];
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

    private Page leafWith( PageCorruption... pageCorruptions )
    {
        return new Page( PageType.LEAF, pageCorruptions );
    }

    private Page internalWith( PageCorruption... pageCorruptions )
    {
        return new Page( PageType.INTERNAL, pageCorruptions );
    }

    private class Page
    {
        private final PageType type;
        private final PageCorruption[] pageCorruptions;

        private Page( PageType type, PageCorruption... pageCorruptions )
        {
            this.type = type;
            this.pageCorruptions = pageCorruptions;
        }

        private void write( PageCursor cursor, CorruptableTreeNode node, int stableGeneration, int unstableGeneration,
                int crashGeneration ) throws IOException
        {
            type.write( cursor, node, oldStableGeneration, stableGeneration );
            Arrays.stream( pageCorruptions )
                    .forEach( pc -> pc.corrupt( cursor, node, stableGeneration, unstableGeneration, crashGeneration ) );
        }
    }

    enum PageType
    {
        LEAF
                {
                    @Override
                    void write( PageCursor cursor, CorruptableTreeNode corruptableTreeNode, int stableGeneration,
                            int unstableGeneration )
                    {
                        TreeNode.initializeLeaf( cursor, stableGeneration, unstableGeneration );
                    }
                },
        INTERNAL
                {
                    @Override
                    void write( PageCursor cursor, CorruptableTreeNode corruptableTreeNode, int stableGeneration,
                            int unstableGeneration )
                    {
                        TreeNode.initializeInternal( cursor, stableGeneration, unstableGeneration );
                        int maxKeyCount = corruptableTreeNode.internalMaxKeyCount();
                        long base = IdSpace.MIN_TREE_NODE_ID;
                        for ( int i = 0; i <= maxKeyCount; i++ )
                        {
                            long child = base + i;
                            corruptableTreeNode.setChildAt( cursor, child, i, stableGeneration, unstableGeneration );
                        }
                        TreeNode.setKeyCount( cursor, maxKeyCount );
                    }
                };

        abstract void write( PageCursor cursor, CorruptableTreeNode corruptableTreeNode,
                int stableGeneration, int unstableGeneration );
    }

    /* GSPPType */
    private GSPPType leftSibling()
    {
        return SimpleGSPPType.LEFT_SIBLING;
    }

    private GSPPType rightSibling()
    {
        return SimpleGSPPType.RIGHT_SIBLING;
    }

    private GSPPType successor()
    {
        return SimpleGSPPType.SUCCESSOR;
    }

    private GSPPType child( int pos )
    {
        return childGSPPType( pos );
    }

    interface GSPPType
    {
        int offset( TreeNode node );
    }

    enum SimpleGSPPType implements GSPPType
    {
        LEFT_SIBLING
                {
                    @Override
                    public int offset( TreeNode node )
                    {
                        return TreeNode.BYTE_POS_LEFTSIBLING;
                    }
                },
        RIGHT_SIBLING
                {
                    @Override
                    public int offset( TreeNode node )
                    {
                        return TreeNode.BYTE_POS_RIGHTSIBLING;
                    }
                },
        SUCCESSOR
                {
                    @Override
                    public int offset( TreeNode node )
                    {
                        return TreeNode.BYTE_POS_SUCCESSOR;
                    }
                }
    }

    private GSPPType childGSPPType( int pos )
    {
        return node -> node.childOffset( pos );
    }

    /* PageCorruption */
    private PageCorruption crashed( GSPPType gsppType )
    {
        return ( pageCursor, node, stableGeneration, unstableGeneration, crashGeneration ) ->
                node.crashGSPP( pageCursor, gsppType.offset( node ), crashGeneration );
    }

    private interface PageCorruption
    {
        void corrupt( PageCursor pageCursor, CorruptableTreeNode node, int stableGeneration,
                int unstableGeneration, int crashGeneration );
    }

    class CorruptableTreeNode extends TreeNode<MutableLong,MutableLong>
    {
        CorruptableTreeNode( int pageSize, Layout<MutableLong,MutableLong> layout )
        {
            super( pageSize, layout );
        }

        void crashGSPP( PageCursor pageCursor, int offset, int crashGeneration )
        {
            pageCursor.setOffset( offset );
            GenerationSafePointerPair.write( pageCursor, 42, stableGeneration, crashGeneration );
        }
    }
}
