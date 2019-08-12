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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.RandomValues;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.GBPTreeConsistencyChecker.assertNoCrashOrBrokenPointerInGSPP;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.MIN_GENERATION;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.goTo;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;
import static org.neo4j.test.rule.PageCacheRule.config;

public class GBPTreeConsistencyCheckerTest
{
    private static final int PAGE_SIZE = 256;
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withAccessChecks( true ) );
    private final RandomRule random = new RandomRule();
    private RandomValues randomValues;
    private TestLayout<MutableLong,MutableLong> layout = SimpleLongLayout.longLayout().build();
    private TestLayout<RawBytes,RawBytes> dynamicLayout = new SimpleByteArrayLayout( false );
    private TreeNode<MutableLong,MutableLong> node;
    private TreeNode<RawBytes,RawBytes> dynamicNode;
    private File indexFile;
    private PageCache pageCache;

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    @Before
    public void setUp()
    {
        indexFile = directory.file( "index" );
        pageCache = createPageCache();
        node = TreeNodeSelector.selectByLayout( layout ).create( PAGE_SIZE, layout );
        dynamicNode = TreeNodeSelector.selectByLayout( dynamicLayout ).create( PAGE_SIZE, dynamicLayout );
        randomValues = random.randomValues();
    }

    private PageCache createPageCache()
    {
        return pageCacheRule.getPageCache( fs.get(), PageCacheRule.config().withPageSize( PAGE_SIZE ) );
    }

    @Test
    public void shouldDetectNotATreeNodeRoot() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long rootNode = visitor.rootNode;
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( rootNode, stableGeneration, unstableGeneration, GBPTreeCorruption.notATreeNode() );

            assertReportNotATreeNode( index, rootNode );
        }
    }

    @Test
    public void shouldDetectNotATreeNodeInternal() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long internalNode = randomValues.among( visitor.internalNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( internalNode, stableGeneration, unstableGeneration, GBPTreeCorruption.notATreeNode() );

            assertReportNotATreeNode( index, internalNode );
        }
    }

    @Test
    public void shouldDetectNotATreeNodeLeaf() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long leafNode = randomValues.among( visitor.leafNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( leafNode, stableGeneration, unstableGeneration, GBPTreeCorruption.notATreeNode() );

            assertReportNotATreeNode( index, leafNode );
        }
    }

    @Test
    public void shouldDetectUnknownTreeNodeTypeRoot() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long rootNode = visitor.rootNode;
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( rootNode, stableGeneration, unstableGeneration, GBPTreeCorruption.unknownTreeNodeType() );

            assertReportUnknownTreeNodeType( index, rootNode );
        }
    }

    @Test
    public void shouldDetectUnknownTreeNodeTypeInternal() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long internalNode = randomValues.among( visitor.internalNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( internalNode, stableGeneration, unstableGeneration, GBPTreeCorruption.unknownTreeNodeType() );

            assertReportUnknownTreeNodeType( index, internalNode );
        }
    }

    @Test
    public void shouldDetectUnknownTreeNodeTypeLeaf() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long leafNode = randomValues.among( visitor.leafNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( leafNode, stableGeneration, unstableGeneration, GBPTreeCorruption.unknownTreeNodeType() );

            assertReportUnknownTreeNodeType( index, leafNode );
        }
    }

    @Test
    public void shouldDetectRightSiblingNotPointingToCorrectSibling() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.leafNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( targetNode, stableGeneration, unstableGeneration, GBPTreeCorruption.rightSiblingPointToNonExisting() );

            assertReportMisalignedSiblingPointers( index );
        }
    }

    @Test
    public void shouldDetectLeftSiblingNotPointingToCorrectSibling() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.leafNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( targetNode, stableGeneration, unstableGeneration, GBPTreeCorruption.leftSiblingPointToNonExisting() );

            assertReportMisalignedSiblingPointers( index );
        }
    }

    @Test
    public void shouldDetectIfAnyNodeInTreeHasSuccessor() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( targetNode, stableGeneration, unstableGeneration, GBPTreeCorruption.hasSuccessor() );

            assertReportPointerToOldVersionOfTreeNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectRightSiblingPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithRightSibling( visitor );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( targetNode, stableGeneration, unstableGeneration, GBPTreeCorruption.rightSiblingPointerHasTooLowGeneration() );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.rightSibling() );
        }
    }

    @Test
    public void shouldDetectLeftSiblingPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithLeftSibling( visitor );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( targetNode, stableGeneration, unstableGeneration, GBPTreeCorruption.leftSiblingPointerHasTooLowGeneration() );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.leftSibling() );
        }
    }

    @Test
    public void shouldDetectChildPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.internalNodes );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            int childPos = randomValues.nextInt( keyCount + 1 );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            corrupt( targetNode, stableGeneration, unstableGeneration, GBPTreeCorruption.childPointerHasTooLowGeneration( childPos ) );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.child( childPos ) );
        }
    }

    @Test
    public void shouldDetectKeysOutOfOrderInIsolatedNode() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithMultipleKeys( visitor );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            int firstKey = randomValues.nextInt( keyCount );
            int secondKey = nextRandomIntExcluding( keyCount, firstKey );
            boolean isLeaf = visitor.leafNodes.contains( targetNode );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> swapKeyOrder = isLeaf ?
                                                                           GBPTreeCorruption.swapKeyOrderLeaf( firstKey, secondKey, keyCount ) :
                                                                           GBPTreeCorruption.swapKeyOrderInternal( firstKey, secondKey, keyCount );
            corrupt( targetNode, stableGeneration, unstableGeneration, swapKeyOrder );

            assertReportKeysOutOfOrderInNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectKeysLocatedInWrongNodeLowKey() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithLeftSibling( visitor );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            int keyPos = randomValues.nextInt( keyCount );
            MutableLong key = new MutableLong( Long.MIN_VALUE );
            boolean isLeaf = visitor.leafNodes.contains( targetNode );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> swapKeyOrder = isLeaf ?
                                                                                     GBPTreeCorruption.overwriteKeyAtPosLeaf( key, keyPos, keyCount ) :
                                                                                     GBPTreeCorruption.overwriteKeyAtPosInternal( key, keyPos, keyCount );
            corrupt( targetNode, stableGeneration, unstableGeneration, swapKeyOrder );

            assertReportKeysLocatedInWrongNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectKeysLocatedInWrongNodeHighKey() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeightTwo( index );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithRightSibling( visitor );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            int keyPos = randomValues.nextInt( keyCount );
            MutableLong key = new MutableLong( Long.MAX_VALUE );
            boolean isLeaf = visitor.leafNodes.contains( targetNode );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> swapKeyOrder = isLeaf ?
                                                                                     GBPTreeCorruption.overwriteKeyAtPosLeaf( key, keyPos, keyCount ) :
                                                                                     GBPTreeCorruption.overwriteKeyAtPosInternal( key, keyPos, keyCount );
            corrupt( targetNode, stableGeneration, unstableGeneration, swapKeyOrder );

            assertReportKeysLocatedInWrongNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeAllocSpaceOverlapActiveKeys() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = index( dynamicLayout).build() )
        {
            treeWithHeightTwo( index, dynamicLayout );

            InspectingVisitor<RawBytes,RawBytes> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            GBPTreeCorruption.PageCorruption<RawBytes,RawBytes> corruption = GBPTreeCorruption.maximizeAllocOffsetInDynamicNode();
            corrupt( targetNode, stableGeneration, unstableGeneration, corruption, dynamicLayout, dynamicNode );

            assertReportAllocSpaceOverlapActiveKeys( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeOverlapBetweenOffsetArrayAndAllocSpace() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = index( dynamicLayout).build() )
        {
            treeWithHeightTwo( index, dynamicLayout );

            InspectingVisitor<RawBytes,RawBytes> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            GBPTreeCorruption.PageCorruption<RawBytes,RawBytes> corruption = GBPTreeCorruption.minimizeAllocOffsetInDynamicNode();
            corrupt( targetNode, stableGeneration, unstableGeneration, corruption, dynamicLayout, dynamicNode );

            assertReportAllocSpaceOverlapOffsetArray( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeSpaceAreasNotSummingToTotalSpace() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = index( dynamicLayout).build() )
        {
            treeWithHeightTwo( index, dynamicLayout );

            InspectingVisitor<RawBytes,RawBytes> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            GBPTreeCorruption.PageCorruption<RawBytes,RawBytes> corruption = GBPTreeCorruption.incrementDeadSpaceInDynamicNode();
            corrupt( targetNode, stableGeneration, unstableGeneration, corruption, dynamicLayout, dynamicNode );

            assertReportSpaceAreasNotSummingToTotalSpace( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeAllocOffsetMisplaced() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = index( dynamicLayout).build() )
        {
            treeWithHeightTwo( index, dynamicLayout );

            InspectingVisitor<RawBytes,RawBytes> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );
            long stableGeneration = visitor.treeState.stableGeneration();
            long unstableGeneration = visitor.treeState.unstableGeneration();

            GBPTreeCorruption.PageCorruption<RawBytes,RawBytes> corruption = GBPTreeCorruption.decrementAllocOffsetInDynamicNode();
            corrupt( targetNode, stableGeneration, unstableGeneration, corruption, dynamicLayout, dynamicNode );

            assertReportAllocOffsetMisplaced( index, targetNode );
        }
    }

    //todo
    //  Tree structure inconsistencies:
    //    > Pointer generation lower than node generation
    //      X Child pointer generation lower than node generation
    //      X Sibling pointer generation lower than node generation
    //    > Sibling pointers don't align
    //      X Sibling pointer generation to low
    //      X Sibling pointer not pointing to correct sibling
    //    > None-tree node
    //      X Root, internal or leaf is none-tree node
    //    > Unknown tree node type
    //      X Root, internal or leaf has unknown tree node type
    //    > Node in tree has successor
    //      X Root, internal or leaf has successor
    //    > Level hierarchy
    //      - Child pointer dont point to next level
    //      - Sibling pointer point to node on other level
    //  Key order inconsistencies:
    //      X Keys out of order in isolated node
    //      X Keys not within parent range
    //  Node meta inconsistency:
    //      X Dynamic layout: Space areas did not sum to total space
    //      X Dynamic layout: Overlap between offsetArray and allocSpace
    //      X Dynamic layout: Overlap between allocSpace and activeKeys
    //      X Dynamic layout: Misplaced allocOffset
    //  Free list inconsistencies:
    //  A page can be either used as a freelist page, used as a tree node (unstable generation), listed in freelist (stable generation), listen in freelist
    //  (unstable generation)
    //      - Page missing from freelist
    //      - Extra page on free list
    //      - Extra empty page in file
    //  Tree meta inconsistencies:
    //    > Can not read meta data.

    @Test
    public void shouldThrowDescriptiveExceptionOnBrokenGSPP() throws Exception
    {
        // GIVEN
        int pageSize = 256;
        PageCursor cursor = new PageAwareByteArrayCursor( pageSize );
        long stableGeneration = MIN_GENERATION;
        long crashGeneration = stableGeneration + 1;
        long unstableGeneration = stableGeneration + 2;
        String pointerFieldName = "abc";
        long pointer = 123;

        cursor.next( 0 );
        new TreeNodeFixedSize<>( pageSize, longLayout().build() ).initializeInternal( cursor, stableGeneration, crashGeneration );
        TreeNode.setSuccessor( cursor, pointer, stableGeneration, crashGeneration );

        // WHEN
        try
        {
            assertNoCrashOrBrokenPointerInGSPP( cursor, stableGeneration, unstableGeneration, pointerFieldName, TreeNode.BYTE_POS_SUCCESSOR );
            cursor.checkAndClearCursorException();
            fail( "Should have failed" );
        }
        catch ( CursorException exception )
        {
            assertThat( exception.getMessage(), allOf( containsString( pointerFieldName ),
                    containsString( pointerFieldName ),
                    containsString( "state=CRASH" ),
                    containsString( "state=EMPTY" ),
                    containsString( String.valueOf( pointer ) ) ) );
        }
    }

    @Test
    public void shouldDetectUnusedPages() throws Exception
    {
        // GIVEN
        int pageSize = 256;
        Layout<MutableLong,MutableLong> layout = longLayout().build();
        TreeNode<MutableLong,MutableLong> node = new TreeNodeFixedSize<>( pageSize, layout );
        long stableGeneration = GenerationSafePointer.MIN_GENERATION;
        long unstableGeneration = stableGeneration + 1;
        PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( pageSize );
        SimpleIdProvider idProvider = new SimpleIdProvider( cursor::duplicate );
        InternalTreeLogic<MutableLong,MutableLong> logic = new InternalTreeLogic<>( idProvider, node, layout, NO_MONITOR );
        cursor.next( idProvider.acquireNewId( stableGeneration, unstableGeneration ) );
        node.initializeLeaf( cursor, stableGeneration, unstableGeneration );
        logic.initialize( cursor );
        StructurePropagation<MutableLong> structure = new StructurePropagation<>( layout.newKey(), layout.newKey(),
                layout.newKey() );
        MutableLong key = layout.newKey();
        for ( int g = 0, k = 0; g < 3; g++ )
        {
            for ( int i = 0; i < 100; i++, k++ )
            {
                key.setValue( k );
                logic.insert( cursor, structure, key, key, ValueMergers.overwrite(),
                        stableGeneration, unstableGeneration );
                if ( structure.hasRightKeyInsert )
                {
                    goTo( cursor, "new root",
                            idProvider.acquireNewId( stableGeneration, unstableGeneration ) );
                    node.initializeInternal( cursor, stableGeneration, unstableGeneration );
                    node.setChildAt( cursor, structure.midChild, 0, stableGeneration, unstableGeneration );
                    node.insertKeyAndRightChildAt( cursor, structure.rightKey, structure.rightChild, 0, 0,
                            stableGeneration, unstableGeneration );
                    TreeNode.setKeyCount( cursor, 1 );
                    logic.initialize( cursor );
                }
                if ( structure.hasMidChildUpdate )
                {
                    logic.initialize( cursor );
                }
                structure.clear();
            }
            stableGeneration = unstableGeneration;
            unstableGeneration++;
        }

        // WHEN
        GBPTreeConsistencyChecker<MutableLong> cc =
                new GBPTreeConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration );
        try
        {
            cc.checkSpace( cursor, idProvider.lastId(), ImmutableEmptyLongIterator.INSTANCE );
            fail( "Should have failed" );
        }
        catch ( RuntimeException exception )
        {
            assertThat( exception.getMessage(), containsString( "unused pages" ) );
        }
    }

    private void corrupt( long targetNode, long stableGeneration, long unstableGeneration,
            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption ) throws IOException
    {
        corrupt( targetNode, stableGeneration, unstableGeneration, corruption, layout, node );
    }

    private <KEY, VALUE> void corrupt( long targetNode, long stableGeneration, long unstableGeneration,
            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption, Layout<KEY,VALUE> layout, TreeNode<KEY,VALUE> node ) throws IOException
    {
        try ( PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize() );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            cursor.next( targetNode );
            corruption.corrupt( cursor, layout, node, stableGeneration, unstableGeneration, unstableGeneration - 1 );
        }
    }

    private long nodeWithLeftSibling( InspectingVisitor visitor )
    {
        List<List<Long>> nodesPerLevel = visitor.nodesPerLevel;
        long targetNode = -1;
        boolean foundNodeWithLeftSibling;
        do
        {
            List<Long> level = randomValues.among( nodesPerLevel );
            if ( level.size() < 2 )
            {
                foundNodeWithLeftSibling = false;
            }
            else
            {
                int index = random.nextInt( level.size() - 1 ) + 1;
                targetNode = level.get( index );
                foundNodeWithLeftSibling = true;
            }
        }
        while ( !foundNodeWithLeftSibling );
        return targetNode;
    }

    private long nodeWithRightSibling( InspectingVisitor visitor )
    {
        List<List<Long>> nodesPerLevel = visitor.nodesPerLevel;
        long targetNode = -1;
        boolean foundNodeWithRightSibling;
        do
        {
            List<Long> level = randomValues.among( nodesPerLevel );
            if ( level.size() < 2 )
            {
                foundNodeWithRightSibling = false;
            }
            else
            {
                int index = random.nextInt( level.size() - 1 );
                targetNode = level.get( index );
                foundNodeWithRightSibling = true;
            }
        }
        while ( !foundNodeWithRightSibling );
        return targetNode;
    }

    private <KEY,VALUE> long nodeWithMultipleKeys( InspectingVisitor<KEY,VALUE> visitor )
    {
        long targetNode;
        int keyCount;
        do
        {
            targetNode = randomValues.among( visitor.allNodes );
            keyCount = visitor.allKeyCounts.get( targetNode );
        }
        while ( keyCount < 2 );
        return targetNode;
    }

    private int nextRandomIntExcluding( int bound, int excluding )
    {
        int result;
        do
        {
            result = randomValues.nextInt( bound );
        }
        while ( result == excluding );
        return result;
    }

    private GBPTreeBuilder<MutableLong,MutableLong> index()
    {
        return index( layout );
    }

    private <KEY,VALUE> GBPTreeBuilder<KEY,VALUE> index( Layout<KEY,VALUE> layout )
    {
        return new GBPTreeBuilder<>( pageCache, indexFile, layout );
    }

    private void treeWithHeightTwo( GBPTree<MutableLong,MutableLong> index ) throws IOException
    {
        treeWithHeightTwo( index, layout );
    }

    private static <KEY,VALUE> void treeWithHeightTwo( GBPTree<KEY,VALUE> index, TestLayout<KEY,VALUE> layout ) throws IOException
    {
        try ( Writer<KEY,VALUE> writer = index.writer() )
        {
            int keyCount = 0;
            while ( getHeight( index ) < 2 )
            {
                writer.put( layout.key( keyCount ), layout.value( keyCount ) );
                keyCount++;
            }
        }
    }

    private static int getHeight( GBPTree<?,?> index ) throws IOException
    {
        InspectingVisitor<?,?> visitor = inspect( index );
        return visitor.lastLevel;
    }

    private static <KEY,VALUE> InspectingVisitor<KEY,VALUE> inspect( GBPTree<KEY,VALUE> index ) throws IOException
    {
        InspectingVisitor<KEY,VALUE> visitor = new InspectingVisitor<>();
        index.visit( visitor );
        return visitor;
    }

    private static void assertReportNotATreeNode( GBPTree<MutableLong,MutableLong> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void notATreeNode( long pageId )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportUnknownTreeNodeType( GBPTree<MutableLong,MutableLong> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void unknownTreeNodeType( long pageId, byte treeNodeType )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportMisalignedSiblingPointers( GBPTree<MutableLong,MutableLong> index ) throws IOException
    {
        MutableBoolean corruptedSiblingPointerCalled = new MutableBoolean();
        MutableBoolean rightmostNodeHasRightSiblingCalled = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration,
                    long leftRightSiblingPointer, long rightNode, long rightNodeGeneration, long rightLeftSiblingPointerGeneration,
                    long rightLeftSiblingPointer )
            {
                corruptedSiblingPointerCalled.setTrue();
            }

            @Override
            public void rightmostNodeHasRightSibling( long rightmostNode, long rightSiblingPointer )
            {
                rightmostNodeHasRightSiblingCalled.setTrue();
            }
        } );
        assertTrue( corruptedSiblingPointerCalled.getValue() || rightmostNodeHasRightSiblingCalled.getValue() );
    }

    private static void assertReportPointerToOldVersionOfTreeNode( GBPTree<MutableLong,MutableLong> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void pointerToOldVersionOfTreeNode( long pageId, long successorPointer )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( GenerationSafePointer.MAX_POINTER, successorPointer );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportPointerGenerationLowerThanNodeGeneration( GBPTree<MutableLong,MutableLong> index, long targetNode,
            GBPTreePointerType expectedPointerType ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointer, long pointerGeneration,
                    long targetNodeGeneration )
            {
                called.setTrue();
                assertEquals( targetNode, sourceNode );
                assertEquals( expectedPointerType, pointerType );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportKeysOutOfOrderInNode( GBPTree<MutableLong,MutableLong> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void keysOutOfOrderInNode( long pageId )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportKeysLocatedInWrongNode( GBPTree<MutableLong,MutableLong> index, long targetNode ) throws IOException
    {
        Set<Long> allNodesWithKeysLocatedInWrongNode = new HashSet<>();
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
        {
            @Override
            public void keysLocatedInWrongNode( long pageId, KeyRange<MutableLong> range, MutableLong key, int pos, int keyCount )
            {
                called.setTrue();
                allNodesWithKeysLocatedInWrongNode.add( pageId );
            }
        } );
        assertCalled( called );
        assertTrue( allNodesWithKeysLocatedInWrongNode.contains( targetNode ) );
    }

    private static void assertReportAllocSpaceOverlapActiveKeys( GBPTree<RawBytes,RawBytes> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                Assert.assertThat( message, containsString( "Overlap between allocSpace and active keys" ) );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportAllocSpaceOverlapOffsetArray( GBPTree<RawBytes,RawBytes> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                Assert.assertThat( message, containsString( "Overlap between offsetArray and allocSpace" ) );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportSpaceAreasNotSummingToTotalSpace( GBPTree<RawBytes,RawBytes> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                Assert.assertThat( message, containsString( "Space areas did not sum to total space" ) );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportAllocOffsetMisplaced( GBPTree<RawBytes,RawBytes> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                Assert.assertThat( message, containsString( "Pointer to allocSpace is misplaced, it should point to start of key" ) );
            }
        } );
        assertCalled( called );
    }

    private static void assertCalled( MutableBoolean called )
    {
        assertTrue( "Expected to receive call to correct consistency report method.", called.getValue() );
    }

    private static class InspectingVisitor<KEY,VALUE> extends GBPTreeVisitor.Adaptor<KEY,VALUE>
    {
        private final List<Long> internalNodes = new ArrayList<>();
        private final List<Long> leafNodes = new ArrayList<>();
        private final List<Long> allNodes = new ArrayList<>();
        private final Map<Long,Integer> allKeyCounts = new HashMap<>();
        private final List<List<Long>> nodesPerLevel = new ArrayList<>();
        private ArrayList<Long> currentLevelNodes;
        private long rootNode;
        private int lastLevel;
        private TreeState treeState;

        private InspectingVisitor()
        {
            clear();
        }

        @Override
        public void treeState( Pair<TreeState,TreeState> statePair )
        {
            this.treeState = TreeStatePair.selectNewestValidState( statePair );
        }

        @Override
        public void beginLevel( int level )
        {
            lastLevel = level;
            currentLevelNodes = new ArrayList<>();
            nodesPerLevel.add( currentLevelNodes );
        }

        @Override
        public void beginNode( long pageId, boolean isLeaf, long generation, int keyCount )
        {
            if ( lastLevel == 0 )
            {
                if ( rootNode != -1 )
                {
                    throw new IllegalStateException( "Expected to only have a single node on level 0" );
                }
                rootNode = pageId;
            }

            currentLevelNodes.add( pageId );
            allNodes.add( pageId );
            allKeyCounts.put( pageId, keyCount );
            if ( isLeaf )
            {
                leafNodes.add( pageId );
            }
            else
            {
                internalNodes.add( pageId );
            }
        }

        private void clear()
        {
            rootNode = -1;
            lastLevel = -1;
        }
    }
}
