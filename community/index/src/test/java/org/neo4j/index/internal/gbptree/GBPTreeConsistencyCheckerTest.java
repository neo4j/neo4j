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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.RandomValues;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
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
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long rootNode = visitor.rootNode;

            index.corrupt( page( rootNode, GBPTreeCorruption.notATreeNode() ) );

            assertReportNotATreeNode( index, rootNode );
        }
    }

    @Test
    public void shouldDetectNotATreeNodeInternal() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long internalNode = randomValues.among( visitor.internalNodes );

            index.corrupt( page( internalNode, GBPTreeCorruption.notATreeNode() ) );

            assertReportNotATreeNode( index, internalNode );
        }
    }

    @Test
    public void shouldDetectNotATreeNodeLeaf() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long leafNode = randomValues.among( visitor.leafNodes );

            index.corrupt( page( leafNode, GBPTreeCorruption.notATreeNode() ) );

            assertReportNotATreeNode( index, leafNode );
        }
    }

    @Test
    public void shouldDetectUnknownTreeNodeTypeRoot() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long rootNode = visitor.rootNode;

            index.corrupt( page( rootNode, GBPTreeCorruption.unknownTreeNodeType() ) );

            assertReportUnknownTreeNodeType( index, rootNode );
        }
    }

    @Test
    public void shouldDetectUnknownTreeNodeTypeInternal() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long internalNode = randomValues.among( visitor.internalNodes );

            index.corrupt( page( internalNode, GBPTreeCorruption.unknownTreeNodeType() ) );

            assertReportUnknownTreeNodeType( index, internalNode );
        }
    }

    @Test
    public void shouldDetectUnknownTreeNodeTypeLeaf() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long leafNode = randomValues.among( visitor.leafNodes );

            index.corrupt( page( leafNode, GBPTreeCorruption.unknownTreeNodeType() ) );

            assertReportUnknownTreeNodeType( index, leafNode );
        }
    }

    @Test
    public void shouldDetectRightSiblingNotPointingToCorrectSibling() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.leafNodes );

            index.corrupt( page( targetNode, GBPTreeCorruption.rightSiblingPointToNonExisting() ) );

            assertReportMisalignedSiblingPointers( index );
        }
    }

    @Test
    public void shouldDetectLeftSiblingNotPointingToCorrectSibling() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.leafNodes );

            index.corrupt( page( targetNode, GBPTreeCorruption.leftSiblingPointToNonExisting() ) );

            assertReportMisalignedSiblingPointers( index );
        }
    }

    @Test
    public void shouldDetectIfAnyNodeInTreeHasSuccessor() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );

            index.corrupt( page( targetNode, GBPTreeCorruption.hasSuccessor() ) );

            assertReportPointerToOldVersionOfTreeNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectRightSiblingPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithRightSibling( visitor );

            index.corrupt( page( targetNode, GBPTreeCorruption.rightSiblingPointerHasTooLowGeneration() ) );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.rightSibling() );
        }
    }

    @Test
    public void shouldDetectLeftSiblingPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithLeftSibling( visitor );

            index.corrupt( page( targetNode, GBPTreeCorruption.leftSiblingPointerHasTooLowGeneration() ) );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.leftSibling() );
        }
    }

    @Test
    public void shouldDetectChildPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.internalNodes );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            int childPos = randomValues.nextInt( keyCount + 1 );

            index.corrupt( page( targetNode, GBPTreeCorruption.childPointerHasTooLowGeneration( childPos ) ) );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.child( childPos ) );
        }
    }

    @Test
    public void shouldDetectKeysOutOfOrderInIsolatedNode() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithMultipleKeys( visitor );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            int firstKey = randomValues.nextInt( keyCount );
            int secondKey = nextRandomIntExcluding( keyCount, firstKey );
            boolean isLeaf = visitor.leafNodes.contains( targetNode );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> swapKeyOrder = isLeaf ?
                                                                                     GBPTreeCorruption.swapKeyOrderLeaf( firstKey, secondKey, keyCount ) :
                                                                                     GBPTreeCorruption.swapKeyOrderInternal( firstKey, secondKey, keyCount );
            index.corrupt( page( targetNode, swapKeyOrder ) );

            assertReportKeysOutOfOrderInNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectKeysLocatedInWrongNodeLowKey() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithLeftSibling( visitor );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            int keyPos = randomValues.nextInt( keyCount );
            MutableLong key = new MutableLong( Long.MIN_VALUE );
            boolean isLeaf = visitor.leafNodes.contains( targetNode );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> swapKeyOrder = isLeaf ?
                                                                                     GBPTreeCorruption.overwriteKeyAtPosLeaf( key, keyPos, keyCount ) :
                                                                                     GBPTreeCorruption.overwriteKeyAtPosInternal( key, keyPos, keyCount );
            index.corrupt( page( targetNode, swapKeyOrder ) );

            assertReportKeysLocatedInWrongNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectKeysLocatedInWrongNodeHighKey() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = nodeWithRightSibling( visitor );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            int keyPos = randomValues.nextInt( keyCount );
            MutableLong key = new MutableLong( Long.MAX_VALUE );
            boolean isLeaf = visitor.leafNodes.contains( targetNode );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> swapKeyOrder = isLeaf ?
                                                                                     GBPTreeCorruption.overwriteKeyAtPosLeaf( key, keyPos, keyCount ) :
                                                                                     GBPTreeCorruption.overwriteKeyAtPosInternal( key, keyPos, keyCount );
            index.corrupt( page( targetNode, swapKeyOrder ) );

            assertReportKeysLocatedInWrongNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeAllocSpaceOverlapActiveKeys() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = index( dynamicLayout ).build() )
        {
            treeWithHeight( index, dynamicLayout, 2 );

            InspectingVisitor<RawBytes,RawBytes> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );

            GBPTreeCorruption.PageCorruption<RawBytes,RawBytes> corruption = GBPTreeCorruption.maximizeAllocOffsetInDynamicNode();
            index.corrupt( page( targetNode, corruption ) );

            assertReportAllocSpaceOverlapActiveKeys( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeOverlapBetweenOffsetArrayAndAllocSpace() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = index( dynamicLayout ).build() )
        {
            treeWithHeight( index, dynamicLayout, 2 );

            InspectingVisitor<RawBytes,RawBytes> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );

            GBPTreeCorruption.PageCorruption<RawBytes,RawBytes> corruption = GBPTreeCorruption.minimizeAllocOffsetInDynamicNode();
            index.corrupt( page( targetNode, corruption ) );

            assertReportAllocSpaceOverlapOffsetArray( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeSpaceAreasNotSummingToTotalSpace() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = index( dynamicLayout ).build() )
        {
            treeWithHeight( index, dynamicLayout, 2 );

            InspectingVisitor<RawBytes,RawBytes> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );

            GBPTreeCorruption.PageCorruption<RawBytes,RawBytes> corruption = GBPTreeCorruption.incrementDeadSpaceInDynamicNode();
            index.corrupt( page( targetNode, corruption ) );

            assertReportSpaceAreasNotSummingToTotalSpace( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeAllocOffsetMisplaced() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = index( dynamicLayout ).build() )
        {
            treeWithHeight( index, dynamicLayout, 2 );

            InspectingVisitor<RawBytes,RawBytes> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );

            GBPTreeCorruption.PageCorruption<RawBytes,RawBytes> corruption = GBPTreeCorruption.decrementAllocOffsetInDynamicNode();
            index.corrupt( page( targetNode, corruption ) );

            assertReportAllocOffsetMisplaced( index, targetNode );
        }
    }

    @Test
    public void shouldDetectPageMissingFreelistEntry() throws IOException
    {
        long targetMissingId;
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                int keyCount = 0;
                while ( getHeight( index ) < 2 )
                {
                    writer.put( layout.key( keyCount ), layout.value( keyCount ) );
                    keyCount++;
                }

                for ( int i = 0; i < keyCount; i++ )
                {
                    writer.remove( layout.key( i ) );
                }
            }
            index.checkpoint( IOLimiter.UNLIMITED );
        }

        // When tree is closed we will overwrite treeState with in memory state so we need to open tree in read only mode for our state corruption to persist.
        try ( GBPTree<MutableLong,MutableLong> index = index().withReadOnly( true ).build() )
        {
            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            int lastIndex = visitor.allFreelistEntries.size() - 1;
            InspectingVisitor<MutableLong,MutableLong>.FreelistEntry lastFreelistEntry = visitor.allFreelistEntries.get( lastIndex );
            targetMissingId = lastFreelistEntry.id;

            GBPTreeCorruption.IndexCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.decrementFreelistWritePos();
            index.corrupt( corruption );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            assertReportUnusedPage( index, targetMissingId );
        }
    }

    @Test
    public void shouldDetectExtraFreelistEntry() throws IOException
    {
        long targetNode;
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                int keyCount = 0;
                while ( getHeight( index ) < 2 )
                {
                    writer.put( layout.key( keyCount ), layout.value( keyCount ) );
                    keyCount++;
                }
            }
            index.checkpoint( IOLimiter.UNLIMITED );
        }

        // When tree is closed we will overwrite treeState with in memory state so we need to open tree in read only mode for our state corruption to persist.
        try ( GBPTree<MutableLong,MutableLong> index = index().withReadOnly( true ).build() )
        {
            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            targetNode = randomValues.among( visitor.allNodes );

            GBPTreeCorruption.IndexCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.addFreelistEntry( targetNode );
            index.corrupt( corruption );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            assertReportActiveTreeNodeInFreelist( index, targetNode );
        }
    }

    @Test
    public void shouldDetectExtraEmptyPageInFile() throws IOException
    {
        long lastId;
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                int keyCount = 0;
                while ( getHeight( index ) < 2 )
                {
                    writer.put( layout.key( keyCount ), layout.value( keyCount ) );
                    keyCount++;
                }
            }
            index.checkpoint( IOLimiter.UNLIMITED );
        }

        // When tree is closed we will overwrite treeState with in memory state so we need to open tree in read only mode for our state corruption to persist.
        try ( GBPTree<MutableLong,MutableLong> index = index().withReadOnly( true ).build() )
        {
            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            TreeState treeState = visitor.treeState;
            lastId = treeState.lastId() + 1;
            TreeState newTreeState = treeStateWithLastId( lastId, treeState );

            GBPTreeCorruption.IndexCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setTreeState( newTreeState );
            index.corrupt( corruption );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            assertReportUnusedPage( index, lastId );
        }
    }

    @Test
    public void shouldDetectIdLargerThanFreelistLastId() throws IOException
    {
        long targetLastId;
        long targetPageId;
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                int keyCount = 0;
                while ( getHeight( index ) < 2 )
                {
                    writer.put( layout.key( keyCount ), layout.value( keyCount ) );
                    keyCount++;
                }
            }
            index.checkpoint( IOLimiter.UNLIMITED );
        }

        // When tree is closed we will overwrite treeState with in memory state so we need to open tree in read only mode for our state corruption to persist.
        try ( GBPTree<MutableLong,MutableLong> index = index().withReadOnly( true ).build() )
        {
            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            TreeState treeState = visitor.treeState;
            targetPageId = treeState.lastId();
            targetLastId = treeState.lastId() - 1;
            TreeState newTreeState = treeStateWithLastId( targetLastId, treeState );

            GBPTreeCorruption.IndexCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setTreeState( newTreeState );
            index.corrupt( corruption );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            assertReportIdExceedLastId( index, targetLastId, targetPageId );
        }
    }

    private static TreeState treeStateWithLastId( long lastId, TreeState treeState )
    {
        return new TreeState( treeState.pageId(), treeState.stableGeneration(), treeState.unstableGeneration(), treeState.rootId(),
                treeState.rootGeneration(), lastId, treeState.freeListWritePageId(), treeState.freeListReadPageId(), treeState.freeListWritePos(),
                treeState.freeListReadPos(), treeState.isClean(), treeState.isValid() );
    }

    @Test
    public void shouldDetectCrashedGSPP() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );
            boolean isLeaf = visitor.leafNodes.contains( targetNode );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            GBPTreePointerType pointerType = randomPointerType( keyCount, isLeaf );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.crashed( pointerType );
            index.corrupt( page( targetNode, corruption ) );

            assertReportCrashedGSPP( index, targetNode, pointerType );
        }
    }

    @Test
    public void shouldDetectBrokenGSPP() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );
            boolean isLeaf = visitor.leafNodes.contains( targetNode );
            int keyCount = visitor.allKeyCounts.get( targetNode );
            GBPTreePointerType pointerType = randomPointerType( keyCount, isLeaf );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.broken( pointerType );
            index.corrupt( page( targetNode, corruption ) );

            assertReportBrokenGSPP( index, targetNode, pointerType );
        }
    }

    @Test
    public void shouldDetectUnreasonableKeyCount() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long targetNode = randomValues.among( visitor.allNodes );
            int unreasonableKeyCount = PAGE_SIZE;

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setKeyCount( unreasonableKeyCount );
            index.corrupt( page( targetNode, corruption ) );

            assertReportUnreasonableKeyCount( index, targetNode, unreasonableKeyCount );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingTwoLevelsDown() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long rootNode = visitor.rootNode;
            int childPos = randomChildPos( visitor, rootNode );
            Long targetChildNode = randomValues.among( visitor.leafNodes );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setChild( childPos, targetChildNode );
            index.corrupt( page( rootNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingToUpperLevelSameStack() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long rootNode = visitor.rootNode;
            Long internalNode = randomValues.among( visitor.nodesPerLevel.get( 1 ) );
            int childPos = randomChildPos( visitor, internalNode );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setChild( childPos, rootNode );
            index.corrupt( page( internalNode, corruption ) );

            assertReportCircularChildPointer( index, rootNode );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingToSameLevel() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            List<Long> internalNodesWithSiblings = visitor.nodesPerLevel.get( 1 );
            long internalNode = randomValues.among( internalNodesWithSiblings );
            long otherInternalNode = randomFromExcluding( internalNodesWithSiblings, internalNode );
            int childPos = randomChildPos( visitor, internalNode );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setChild( childPos, otherInternalNode );
            index.corrupt( page( internalNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingToUpperLevelNotSameStack() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 3 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long upperInternalNode = randomValues.among( visitor.nodesPerLevel.get( 1 ) );
            long lowerInternalNode = randomValues.among( visitor.nodesPerLevel.get( 2 ) );
            int childPos = randomChildPos( visitor, lowerInternalNode );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setChild( childPos, upperInternalNode );
            index.corrupt( page( lowerInternalNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingToChildOwnedByOtherNode() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            List<Long> internalNodesWithSiblings = visitor.nodesPerLevel.get( 1 );
            long internalNode = randomValues.among( internalNodesWithSiblings );
            long otherInternalNode = randomFromExcluding( internalNodesWithSiblings, internalNode );
            int otherChildPos = randomChildPos( visitor, otherInternalNode );
            long childInOtherInternal = childAt( otherInternalNode, otherChildPos, visitor.treeState );
            int childPos = randomChildPos( visitor, internalNode );

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setChild( childPos, childInOtherInternal );
            index.corrupt( page( internalNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectSiblingPointerPointingToLowerLevel() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long internalNode = randomValues.among( visitor.internalNodes );
            long leafNode = randomValues.among( visitor.leafNodes );
            GBPTreePointerType siblingPointer = randomSiblingPointerType();

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setPointer( siblingPointer, leafNode );
            index.corrupt( page( internalNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectSiblingPointerPointingToUpperLevel() throws IOException
    {
        try ( GBPTree<MutableLong,MutableLong> index = index().build() )
        {
            treeWithHeight( index, 2 );

            InspectingVisitor<MutableLong,MutableLong> visitor = inspect( index );
            long internalNode = randomValues.among( visitor.internalNodes );
            long leafNode = randomValues.among( visitor.leafNodes );
            GBPTreePointerType siblingPointer = randomSiblingPointerType();

            GBPTreeCorruption.PageCorruption<MutableLong,MutableLong> corruption = GBPTreeCorruption.setPointer( siblingPointer, internalNode );
            index.corrupt( page( leafNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
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
    //    > Broken GSPP
    //      X Should detect crashed GSPP
    //      X Should detect broken GSPP
    //      - Should not report crashed GSPP if allowed
    //    > Level hierarchy
    //      X Child pointer point two levels down
    //      X Child pointer point to upper level same stack
    //      X Child pointer point to same level
    //      X Child pointer point to upper level not same stack
    //      X Child pointer point to child owned by other internal node
    //      X Sibling pointer point to lower level
    //      X Sibling pointer point to upper level
    //  Key order inconsistencies:
    //      X Keys out of order in isolated node
    //      X Keys not within parent range
    //  Node meta inconsistency:
    //      X Dynamic layout: Space areas did not sum to total space
    //      X Dynamic layout: Overlap between offsetArray and allocSpace
    //      X Dynamic layout: Overlap between allocSpace and activeKeys
    //      X Dynamic layout: Misplaced allocOffset
    //      X Unreasonable keyCount
    //  Free list inconsistencies:
    //  A page can be either used as a freelist page, used as a tree node (unstable generation), listed in freelist (stable generation), listen in freelist
    //  (unstable generation)
    //      X Page missing from freelist
    //      X Extra page on free list
    //      X Extra empty page in file
    //  Tree meta inconsistencies:
    //    > Can not read meta data.

    private static <KEY, VALUE> GBPTreeCorruption.IndexCorruption<KEY,VALUE> page( long targetNode, GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption )
    {
        return GBPTreeCorruption.pageSpecificCorruption( targetNode, corruption );
    }

    private <KEY, VALUE> long nodeWithLeftSibling( InspectingVisitor<KEY,VALUE> visitor )
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

    private <KEY, VALUE> long nodeWithRightSibling( InspectingVisitor<KEY,VALUE> visitor )
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

    private <KEY, VALUE> long nodeWithMultipleKeys( InspectingVisitor<KEY,VALUE> visitor )
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

    private <T> T randomFromExcluding( List<T> from, T excluding )
    {
        T other;
        do
        {
            other = randomValues.among( from );
        }
        while ( other.equals( excluding ) );
        return other;
    }

    private <KEY, VALUE> int randomChildPos( InspectingVisitor<KEY,VALUE> visitor, long internalNode )
    {
        int childCount = visitor.allKeyCounts.get( internalNode ) + 1;
        return randomValues.nextInt( childCount );
    }

    private GBPTreePointerType randomSiblingPointerType()
    {
        return randomValues.among( Arrays.asList( GBPTreePointerType.leftSibling(), GBPTreePointerType.rightSibling() ) );
    }

    private long childAt( long internalNode, int childPos, TreeState treeState ) throws IOException
    {
        try ( PagedFile pagedFile = pageCache.map( indexFile, pageCache.pageSize() );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            PageCursorUtil.goTo( cursor, "", internalNode );
            return pointer( node.childAt( cursor, childPos, treeState.stableGeneration(), treeState.unstableGeneration() ) );
        }
    }

    private GBPTreeBuilder<MutableLong,MutableLong> index()
    {
        return index( layout );
    }

    private <KEY, VALUE> GBPTreeBuilder<KEY,VALUE> index( Layout<KEY,VALUE> layout )
    {
        return new GBPTreeBuilder<>( pageCache, indexFile, layout );
    }

    private GBPTreePointerType randomPointerType( int keyCount, boolean isLeaf )
    {
        int bound = isLeaf ? 3 : 4;
        switch ( randomValues.nextInt( bound ) )
        {
        case 0:
            return GBPTreePointerType.leftSibling();
        case 1:
            return GBPTreePointerType.rightSibling();
        case 2:
            return GBPTreePointerType.successor();
        case 3:
            return GBPTreePointerType.child( randomValues.nextInt( keyCount + 1 ) );
        default:
            throw new IllegalStateException( "Unrecognized option" );
        }
    }

    private void treeWithHeight( GBPTree<MutableLong,MutableLong> index, int height ) throws IOException
    {
        treeWithHeight( index, layout, height );
    }

    private static <KEY, VALUE> void treeWithHeight( GBPTree<KEY,VALUE> index, TestLayout<KEY,VALUE> layout, int height ) throws IOException
    {
        try ( Writer<KEY,VALUE> writer = index.writer() )
        {
            int keyCount = 0;
            while ( getHeight( index ) < height )
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

    private static <KEY, VALUE> InspectingVisitor<KEY,VALUE> inspect( GBPTree<KEY,VALUE> index ) throws IOException
    {
        InspectingVisitor<KEY,VALUE> visitor = new InspectingVisitor<>();
        index.visit( visitor );
        return visitor;
    }

    private static void assertReportNotATreeNode( GBPTree<MutableLong,MutableLong> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
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
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
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
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
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
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
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
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
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
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
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
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<RawBytes>()
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
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<RawBytes>()
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
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<RawBytes>()
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
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<RawBytes>()
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

    private static void assertReportUnusedPage( GBPTree<MutableLong,MutableLong> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
        {
            @Override
            public void unusedPage( long pageId )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportActiveTreeNodeInFreelist( GBPTree<MutableLong,MutableLong> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
        {
            @Override
            public void pageIdSeenMultipleTimes( long pageId )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportIdExceedLastId( GBPTree<MutableLong,MutableLong> index, long targetLastId, long targetPageId ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
        {
            @Override
            public void pageIdExceedLastId( long lastId, long pageId )
            {
                called.setTrue();
                assertEquals( targetLastId, lastId );
                assertEquals( targetPageId, pageId );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportCrashedGSPP( GBPTree<MutableLong,MutableLong> index, long targetNode, GBPTreePointerType targetPointerType )
            throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
        {
            @Override
            public void crashedPointer( long pageId, GBPTreePointerType pointerType,
                    long generationA, long readPointerA, long pointerA, byte stateA,
                    long generationB, long readPointerB, long pointerB, byte stateB )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( targetPointerType, pointerType );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportBrokenGSPP( GBPTree<MutableLong,MutableLong> index, long targetNode, GBPTreePointerType targetPointerType )
            throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
        {
            @Override
            public void brokenPointer( long pageId, GBPTreePointerType pointerType,
                    long generationA, long readPointerA, long pointerA, byte stateA,
                    long generationB, long readPointerB, long pointerB, byte stateB )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( targetPointerType, pointerType );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportUnreasonableKeyCount( GBPTree<MutableLong,MutableLong> index, long targetNode, int targetKeyCount ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
        {
            @Override
            public void unreasonableKeyCount( long pageId, int keyCount )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( targetKeyCount, keyCount );
            }
        } );
        assertCalled( called );
    }

    private static void assertReportAnyStructuralInconsistency( GBPTree<MutableLong,MutableLong> index ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
        {
            @Override
            public void rightmostNodeHasRightSibling( long rightmostNode, long rightSiblingPointer )
            {
                called.setTrue();
            }

            @Override
            public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration,
                    long leftRightSiblingPointer, long rightNode, long rightNodeGeneration, long rightLeftSiblingPointerGeneration,
                    long rightLeftSiblingPointer )
            {
                called.setTrue();
            }

            @Override
            public void keysLocatedInWrongNode( long pageId, KeyRange<MutableLong> range, MutableLong mutableLong, int pos, int keyCount )
            {
                called.setTrue();
            }

            @Override
            public void pageIdSeenMultipleTimes( long pageId )
            {
                called.setTrue();
            }

            @Override
            public void childNodeFoundAmongParentNodes( int level, long pageId, KeyRange<MutableLong> superRange )
            {
                called.setTrue();
            }
        } );
        assertCalled( called );
    }

    private static void assertReportCircularChildPointer( GBPTree<MutableLong,MutableLong> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<MutableLong>()
        {
            @Override
            public void childNodeFoundAmongParentNodes( int level, long pageId, KeyRange<MutableLong> superRange )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static void assertCalled( MutableBoolean called )
    {
        assertTrue( "Expected to receive call to correct consistency report method.", called.getValue() );
    }

    private static class InspectingVisitor<KEY, VALUE> extends GBPTreeVisitor.Adaptor<KEY,VALUE>
    {
        private final List<Long> internalNodes = new ArrayList<>();
        private final List<Long> leafNodes = new ArrayList<>();
        private final List<Long> allNodes = new ArrayList<>();
        private final Map<Long,Integer> allKeyCounts = new HashMap<>();
        private final List<List<Long>> nodesPerLevel = new ArrayList<>();
        private final List<FreelistEntry> allFreelistEntries = new ArrayList<>();
        private ArrayList<Long> currentLevelNodes;
        private long currentFreelistPage;
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

        @Override
        public void beginFreelistPage( long pageId )
        {
            currentFreelistPage = pageId;
        }

        @Override
        public void freelistEntry( long pageId, long generation, int pos )
        {
            allFreelistEntries.add( new FreelistEntry( currentFreelistPage, pos, pageId, generation ) );
        }

        private void clear()
        {
            rootNode = -1;
            lastLevel = -1;
        }

        private class FreelistEntry
        {
            private final long freelistPageId;
            private final int pos;
            private final long id;
            private final long generation;

            private FreelistEntry( long freelistPageId, int pos, long id, long generation )
            {
                this.freelistPageId = freelistPageId;
                this.pos = pos;
                this.id = id;
                this.generation = generation;
            }
        }
    }
}
