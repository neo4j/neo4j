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
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.LongList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
import static org.junit.Assume.assumeTrue;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.test.rule.PageCacheRule.config;

public abstract class GBPTreeConsistencyCheckerTestBase<KEY,VALUE>
{
    private static final int PAGE_SIZE = 256;
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withAccessChecks( true ) );
    private final RandomRule random = new RandomRule();
    private RandomValues randomValues;
    private TestLayout<KEY,VALUE> layout;
    private TreeNode<KEY,VALUE> node;
    private File indexFile;
    private PageCache pageCache;
    private boolean isDynamic;

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    @Before
    public void setUp()
    {
        indexFile = directory.file( "index" );
        pageCache = createPageCache();
        layout = getLayout();
        node = TreeNodeSelector.selectByLayout( layout ).create( PAGE_SIZE, layout );
        randomValues = random.randomValues();
        isDynamic = node instanceof TreeNodeDynamicSize;
    }

    protected abstract TestLayout<KEY,VALUE> getLayout();

    private PageCache createPageCache()
    {
        return pageCacheRule.getPageCache( fs.get(), PageCacheRule.config().withPageSize( PAGE_SIZE ) );
    }

    @Test
    public void shouldDetectNotATreeNodeRoot() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long rootNode = inspection.getRootNode();

            index.unsafe( page( rootNode, GBPTreeCorruption.notATreeNode() ) );

            assertReportNotATreeNode( index, rootNode );
        }
    }

    @Test
    public void shouldDetectNotATreeNodeInternal() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            final ImmutableLongList internalNodes = inspection.getInternalNodes();
            long internalNode = randomAmong( internalNodes );

            index.unsafe( page( internalNode, GBPTreeCorruption.notATreeNode() ) );

            assertReportNotATreeNode( index, internalNode );
        }
    }

    @Test
    public void shouldDetectNotATreeNodeLeaf() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long leafNode = randomAmong( inspection.getLeafNodes() );

            index.unsafe( page( leafNode, GBPTreeCorruption.notATreeNode() ) );

            assertReportNotATreeNode( index, leafNode );
        }
    }

    @Test
    public void shouldDetectUnknownTreeNodeTypeRoot() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long rootNode = inspection.getRootNode();

            index.unsafe( page( rootNode, GBPTreeCorruption.unknownTreeNodeType() ) );

            assertReportUnknownTreeNodeType( index, rootNode );
        }
    }

    @Test
    public void shouldDetectUnknownTreeNodeTypeInternal() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long internalNode = randomAmong( inspection.getInternalNodes() );

            index.unsafe( page( internalNode, GBPTreeCorruption.unknownTreeNodeType() ) );

            assertReportUnknownTreeNodeType( index, internalNode );
        }
    }

    @Test
    public void shouldDetectUnknownTreeNodeTypeLeaf() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long leafNode = randomAmong( inspection.getLeafNodes() );

            index.unsafe( page( leafNode, GBPTreeCorruption.unknownTreeNodeType() ) );

            assertReportUnknownTreeNodeType( index, leafNode );
        }
    }

    @Test
    public void shouldDetectRightSiblingNotPointingToCorrectSibling() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getLeafNodes() );

            index.unsafe( page( targetNode, GBPTreeCorruption.rightSiblingPointToNonExisting() ) );

            assertReportMisalignedSiblingPointers( index );
        }
    }

    @Test
    public void shouldDetectLeftSiblingNotPointingToCorrectSibling() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getLeafNodes() );

            index.unsafe( page( targetNode, GBPTreeCorruption.leftSiblingPointToNonExisting() ) );

            assertReportMisalignedSiblingPointers( index );
        }
    }

    @Test
    public void shouldDetectIfAnyNodeInTreeHasSuccessor() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getAllNodes() );

            index.unsafe( page( targetNode, GBPTreeCorruption.hasSuccessor() ) );

            assertReportPointerToOldVersionOfTreeNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectRightSiblingPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = nodeWithRightSibling( inspection );

            index.unsafe( page( targetNode, GBPTreeCorruption.rightSiblingPointerHasTooLowGeneration() ) );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.rightSibling() );
        }
    }

    @Test
    public void shouldDetectLeftSiblingPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = nodeWithLeftSibling( inspection );

            index.unsafe( page( targetNode, GBPTreeCorruption.leftSiblingPointerHasTooLowGeneration() ) );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.leftSibling() );
        }
    }

    @Test
    public void shouldDetectChildPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getInternalNodes() );
            int keyCount = inspection.getKeyCounts().get( targetNode );
            int childPos = randomValues.nextInt( keyCount + 1 );

            index.unsafe( page( targetNode, GBPTreeCorruption.childPointerHasTooLowGeneration( childPos ) ) );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.child( childPos ) );
        }
    }

    @Test
    public void shouldDetectKeysOutOfOrderInIsolatedNode() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = nodeWithMultipleKeys( inspection );
            int keyCount = inspection.getKeyCounts().get( targetNode );
            int firstKey = randomValues.nextInt( keyCount );
            int secondKey = nextRandomIntExcluding( keyCount, firstKey );
            boolean isLeaf = inspection.getLeafNodes().contains( targetNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> swapKeyOrder = isLeaf ?
                                                                                     GBPTreeCorruption.swapKeyOrderLeaf( firstKey, secondKey, keyCount ) :
                                                                                     GBPTreeCorruption.swapKeyOrderInternal( firstKey, secondKey, keyCount );
            index.unsafe( page( targetNode, swapKeyOrder ) );

            assertReportKeysOutOfOrderInNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectKeysLocatedInWrongNodeLowKey() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = nodeWithLeftSibling( inspection );
            int keyCount = inspection.getKeyCounts().get( targetNode );
            int keyPos = randomValues.nextInt( keyCount );
            KEY key = layout.key( Long.MIN_VALUE );
            boolean isLeaf = inspection.getLeafNodes().contains( targetNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> swapKeyOrder = isLeaf ?
                                                                                     GBPTreeCorruption.overwriteKeyAtPosLeaf( key, keyPos, keyCount ) :
                                                                                     GBPTreeCorruption.overwriteKeyAtPosInternal( key, keyPos, keyCount );
            index.unsafe( page( targetNode, swapKeyOrder ) );

            assertReportKeysLocatedInWrongNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectKeysLocatedInWrongNodeHighKey() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = nodeWithRightSibling( inspection );
            int keyCount = inspection.getKeyCounts().get( targetNode );
            int keyPos = randomValues.nextInt( keyCount );
            KEY key = layout.key( Long.MAX_VALUE );
            boolean isLeaf = inspection.getLeafNodes().contains( targetNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> swapKeyOrder = isLeaf ?
                                                                                     GBPTreeCorruption.overwriteKeyAtPosLeaf( key, keyPos, keyCount ) :
                                                                                     GBPTreeCorruption.overwriteKeyAtPosInternal( key, keyPos, keyCount );
            index.unsafe( page( targetNode, swapKeyOrder ) );

            assertReportKeysLocatedInWrongNode( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeAllocSpaceOverlapActiveKeys() throws IOException
    {
        assumeTrue( "Only relevant for dynamic layout", isDynamic );
        try ( GBPTree<KEY,VALUE> index = index( layout ).build() )
        {
            treeWithHeight( index, layout, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getAllNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.maximizeAllocOffsetInDynamicNode();
            index.unsafe( page( targetNode, corruption ) );

            assertReportAllocSpaceOverlapActiveKeys( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeOverlapBetweenOffsetArrayAndAllocSpace() throws IOException
    {
        assumeTrue( "Only relevant for dynamic layout", isDynamic );
        try ( GBPTree<KEY,VALUE> index = index( layout ).build() )
        {
            treeWithHeight( index, layout, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getAllNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.minimizeAllocOffsetInDynamicNode();
            index.unsafe( page( targetNode, corruption ) );

            assertReportAllocSpaceOverlapOffsetArray( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeSpaceAreasNotSummingToTotalSpace() throws IOException
    {
        assumeTrue( "Only relevant for dynamic layout", isDynamic );
        try ( GBPTree<KEY,VALUE> index = index( layout ).build() )
        {
            treeWithHeight( index, layout, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getAllNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.incrementDeadSpaceInDynamicNode();
            index.unsafe( page( targetNode, corruption ) );

            assertReportSpaceAreasNotSummingToTotalSpace( index, targetNode );
        }
    }

    @Test
    public void shouldDetectNodeMetaInconsistencyDynamicNodeAllocOffsetMisplaced() throws IOException
    {
        assumeTrue( "Only relevant for dynamic layout", isDynamic );
        try ( GBPTree<KEY,VALUE> index = index( layout ).build() )
        {
            treeWithHeight( index, layout, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getAllNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.decrementAllocOffsetInDynamicNode();
            index.unsafe( page( targetNode, corruption ) );

            assertReportAllocOffsetMisplaced( index, targetNode );
        }
    }

    @Test
    public void shouldDetectPageMissingFreelistEntry() throws IOException
    {
        long targetMissingId;
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<KEY,VALUE> writer = index.writer() )
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
        try ( GBPTree<KEY,VALUE> index = index().withReadOnly( true ).build() )
        {
            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            int lastIndex = inspection.getAllFreelistEntries().size() - 1;
            InspectingVisitor.FreelistEntry lastFreelistEntry = inspection.getAllFreelistEntries().get( lastIndex );
            targetMissingId = lastFreelistEntry.id;

            GBPTreeCorruption.IndexCorruption<KEY,VALUE> corruption = GBPTreeCorruption.decrementFreelistWritePos();
            index.unsafe( corruption );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            assertReportUnusedPage( index, targetMissingId );
        }
    }

    @Test
    public void shouldDetectExtraFreelistEntry() throws IOException
    {
        long targetNode;
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<KEY,VALUE> writer = index.writer() )
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
        try ( GBPTree<KEY,VALUE> index = index().withReadOnly( true ).build() )
        {
            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            targetNode = randomAmong( inspection.getAllNodes() );

            GBPTreeCorruption.IndexCorruption<KEY,VALUE> corruption = GBPTreeCorruption.addFreelistEntry( targetNode );
            index.unsafe( corruption );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            assertReportActiveTreeNodeInFreelist( index, targetNode );
        }
    }

    @Test
    public void shouldDetectExtraEmptyPageInFile() throws IOException
    {
        long lastId;
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<KEY,VALUE> writer = index.writer() )
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
        try ( GBPTree<KEY,VALUE> index = index().withReadOnly( true ).build() )
        {
            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            TreeState treeState = inspection.getTreeState();
            lastId = treeState.lastId() + 1;
            TreeState newTreeState = treeStateWithLastId( lastId, treeState );

            GBPTreeCorruption.IndexCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setTreeState( newTreeState );
            index.unsafe( corruption );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            assertReportUnusedPage( index, lastId );
        }
    }

    @Test
    public void shouldDetectIdLargerThanFreelistLastId() throws IOException
    {
        long targetLastId;
        long targetPageId;
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<KEY,VALUE> writer = index.writer() )
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
        try ( GBPTree<KEY,VALUE> index = index().withReadOnly( true ).build() )
        {
            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            TreeState treeState = inspection.getTreeState();
            targetPageId = treeState.lastId();
            targetLastId = treeState.lastId() - 1;
            TreeState newTreeState = treeStateWithLastId( targetLastId, treeState );

            GBPTreeCorruption.IndexCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setTreeState( newTreeState );
            index.unsafe( corruption );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<KEY,VALUE> index = index().build() )
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
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getAllNodes() );
            boolean isLeaf = inspection.getLeafNodes().contains( targetNode );
            int keyCount = inspection.getKeyCounts().get( targetNode );
            GBPTreePointerType pointerType = randomPointerType( keyCount, isLeaf );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.crashed( pointerType );
            index.unsafe( page( targetNode, corruption ) );

            assertReportCrashedGSPP( index, targetNode, pointerType );
        }
    }

    @Test
    public void shouldDetectBrokenGSPP() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getAllNodes() );
            boolean isLeaf = inspection.getLeafNodes().contains( targetNode );
            int keyCount = inspection.getKeyCounts().get( targetNode );
            GBPTreePointerType pointerType = randomPointerType( keyCount, isLeaf );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.broken( pointerType );
            index.unsafe( page( targetNode, corruption ) );

            assertReportBrokenGSPP( index, targetNode, pointerType );
        }
    }

    @Test
    public void shouldDetectUnreasonableKeyCount() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long targetNode = randomAmong( inspection.getAllNodes() );
            int unreasonableKeyCount = PAGE_SIZE;

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setKeyCount( unreasonableKeyCount );
            index.unsafe( page( targetNode, corruption ) );

            assertReportUnreasonableKeyCount( index, targetNode, unreasonableKeyCount );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingTwoLevelsDown() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long rootNode = inspection.getRootNode();
            int childPos = randomChildPos( inspection, rootNode );
            long targetChildNode = randomAmong( inspection.getLeafNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setChild( childPos, targetChildNode );
            index.unsafe( page( rootNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingToUpperLevelSameStack() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long rootNode = inspection.getRootNode();
            Long internalNode = randomAmong( inspection.getNodesPerLevel().get( 1 ) );
            int childPos = randomChildPos( inspection, internalNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setChild( childPos, rootNode );
            index.unsafe( page( internalNode, corruption ) );

            assertReportCircularChildPointer( index, rootNode );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingToSameLevel() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            ImmutableLongList internalNodesWithSiblings = inspection.getNodesPerLevel().get( 1 );
            long internalNode = randomAmong( internalNodesWithSiblings );
            long otherInternalNode = randomFromExcluding( internalNodesWithSiblings, internalNode );
            int childPos = randomChildPos( inspection, internalNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setChild( childPos, otherInternalNode );
            index.unsafe( page( internalNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingToUpperLevelNotSameStack() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 3 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long upperInternalNode = randomAmong( inspection.getNodesPerLevel().get( 1 ) );
            long lowerInternalNode = randomAmong( inspection.getNodesPerLevel().get( 2 ) );
            int childPos = randomChildPos( inspection, lowerInternalNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setChild( childPos, upperInternalNode );
            index.unsafe( page( lowerInternalNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectChildPointerPointingToChildOwnedByOtherNode() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            LongList internalNodesWithSiblings = inspection.getNodesPerLevel().get( 1 );
            long internalNode = randomAmong( internalNodesWithSiblings );
            long otherInternalNode = randomFromExcluding( internalNodesWithSiblings, internalNode );
            int otherChildPos = randomChildPos( inspection, otherInternalNode );
            long childInOtherInternal = childAt( otherInternalNode, otherChildPos, inspection.getTreeState() );
            int childPos = randomChildPos( inspection, internalNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setChild( childPos, childInOtherInternal );
            index.unsafe( page( internalNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectExceptionDuringConsistencyCheck() throws IOException
    {
        assumeTrue( "This trick to make GBPTreeConsistencyChecker throw exception only work for dynamic layout", isDynamic );
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            final long leaf = randomAmong( inspection.getLeafNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setHighestReasonableKeyCount();
            index.unsafe( page( leaf, corruption ) );

            assertReportException( index );
        }
    }

    @Test
    public void shouldDetectSiblingPointerPointingToLowerLevel() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long internalNode = randomAmong( inspection.getInternalNodes() );
            long leafNode = randomAmong( inspection.getLeafNodes() );
            GBPTreePointerType siblingPointer = randomSiblingPointerType();

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setPointer( siblingPointer, leafNode );
            index.unsafe( page( internalNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    public void shouldDetectSiblingPointerPointingToUpperLevel() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
            long internalNode = randomAmong( inspection.getInternalNodes() );
            long leafNode = randomAmong( inspection.getLeafNodes() );
            GBPTreePointerType siblingPointer = randomSiblingPointerType();

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setPointer( siblingPointer, internalNode );
            index.unsafe( page( leafNode, corruption ) );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    private static <KEY, VALUE> GBPTreeCorruption.IndexCorruption<KEY,VALUE> page( long targetNode, GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption )
    {
        return GBPTreeCorruption.pageSpecificCorruption( targetNode, corruption );
    }

    private long nodeWithLeftSibling( GBPTreeInspection<KEY,VALUE> visitor )
    {
        List<ImmutableLongList> nodesPerLevel = visitor.getNodesPerLevel();
        long targetNode = -1;
        boolean foundNodeWithLeftSibling;
        do
        {
            ImmutableLongList level = randomValues.among( nodesPerLevel );
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

    private long nodeWithRightSibling( GBPTreeInspection<KEY,VALUE> inspection )
    {
        List<ImmutableLongList> nodesPerLevel = inspection.getNodesPerLevel();
        long targetNode = -1;
        boolean foundNodeWithRightSibling;
        do
        {
            ImmutableLongList level = randomValues.among( nodesPerLevel );
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

    private long nodeWithMultipleKeys( GBPTreeInspection<KEY,VALUE> inspection )
    {
        long targetNode;
        int keyCount;
        do
        {
            targetNode = randomAmong( inspection.getAllNodes() );
            keyCount = inspection.getKeyCounts().get( targetNode );
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

    private long randomFromExcluding( LongList from, long excluding )
    {
        long other;
        do
        {
            other = randomAmong( from );
        }
        while ( other == excluding );
        return other;
    }

    private int randomChildPos( GBPTreeInspection<KEY,VALUE> inspection, long internalNode )
    {
        int childCount = inspection.getKeyCounts().get( internalNode ) + 1;
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

    private GBPTreeBuilder<KEY,VALUE> index()
    {
        return index( layout );
    }

    private GBPTreeBuilder<KEY,VALUE> index( Layout<KEY,VALUE> layout )
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

    private void treeWithHeight( GBPTree<KEY,VALUE> index, int height ) throws IOException
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

    private static <KEY,VALUE> int getHeight( GBPTree<KEY,VALUE> index ) throws IOException
    {
        GBPTreeInspection<KEY,VALUE> inspection = inspect( index );
        return inspection.getLastLevel();
    }

    private static <KEY, VALUE> GBPTreeInspection<KEY,VALUE> inspect( GBPTree<KEY,VALUE> index ) throws IOException
    {
        return index.visit( new InspectingVisitor<>() ).get();
    }

    private static <KEY,VALUE> void assertReportNotATreeNode( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void notATreeNode( long pageId, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportUnknownTreeNodeType( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void unknownTreeNodeType( long pageId, byte treeNodeType, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportMisalignedSiblingPointers( GBPTree<KEY,VALUE> index ) throws IOException
    {
        MutableBoolean corruptedSiblingPointerCalled = new MutableBoolean();
        MutableBoolean rightmostNodeHasRightSiblingCalled = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration,
                    long leftRightSiblingPointer, long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode,
                    long rightNodeGeneration, File file )
            {
                corruptedSiblingPointerCalled.setTrue();
            }

            @Override
            public void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode, File file )
            {
                rightmostNodeHasRightSiblingCalled.setTrue();
            }
        } );
        assertTrue( corruptedSiblingPointerCalled.getValue() || rightmostNodeHasRightSiblingCalled.getValue() );
    }

    private static <KEY,VALUE> void assertReportPointerToOldVersionOfTreeNode( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void pointerToOldVersionOfTreeNode( long pageId, long successorPointer, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( GenerationSafePointer.MAX_POINTER, successorPointer );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportPointerGenerationLowerThanNodeGeneration( GBPTree<KEY,VALUE> index, long targetNode,
            GBPTreePointerType expectedPointerType ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer,
                    long targetNodeGeneration, File file )
            {
                called.setTrue();
                assertEquals( targetNode, sourceNode );
                assertEquals( expectedPointerType, pointerType );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportKeysOutOfOrderInNode( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void keysOutOfOrderInNode( long pageId, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportKeysLocatedInWrongNode( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        Set<Long> allNodesWithKeysLocatedInWrongNode = new HashSet<>();
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId, File file )
            {
                called.setTrue();
                allNodesWithKeysLocatedInWrongNode.add( pageId );
            }
        } );
        assertCalled( called );
        assertTrue( allNodesWithKeysLocatedInWrongNode.contains( targetNode ) );
    }

    private static <KEY,VALUE> void assertReportAllocSpaceOverlapActiveKeys( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                Assert.assertThat( message, containsString( "Overlap between allocSpace and active keys" ) );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportAllocSpaceOverlapOffsetArray( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                Assert.assertThat( message, containsString( "Overlap between offsetArray and allocSpace" ) );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportSpaceAreasNotSummingToTotalSpace( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                Assert.assertThat( message, containsString( "Space areas did not sum to total space" ) );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportAllocOffsetMisplaced( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                Assert.assertThat( message, containsString( "Pointer to allocSpace is misplaced, it should point to start of key" ) );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportUnusedPage( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void unusedPage( long pageId, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportActiveTreeNodeInFreelist( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void pageIdSeenMultipleTimes( long pageId, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportIdExceedLastId( GBPTree<KEY,VALUE> index, long targetLastId, long targetPageId ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void pageIdExceedLastId( long lastId, long pageId, File file )
            {
                called.setTrue();
                assertEquals( targetLastId, lastId );
                assertEquals( targetPageId, pageId );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportCrashedGSPP( GBPTree<KEY,VALUE> index, long targetNode, GBPTreePointerType targetPointerType )
            throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void crashedPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
                    long generationB, long readPointerB, long pointerB, byte stateB, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( targetPointerType, pointerType );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportBrokenGSPP( GBPTree<KEY,VALUE> index, long targetNode, GBPTreePointerType targetPointerType )
            throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void brokenPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
                    long generationB, long readPointerB, long pointerB, byte stateB, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( targetPointerType, pointerType );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportUnreasonableKeyCount( GBPTree<KEY,VALUE> index, long targetNode, int targetKeyCount ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void unreasonableKeyCount( long pageId, int keyCount, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( targetKeyCount, keyCount );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportAnyStructuralInconsistency( GBPTree<KEY,VALUE> index ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode, File file )
            {
                called.setTrue();
            }

            @Override
            public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration,
                    long leftRightSiblingPointer, long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode,
                    long rightNodeGeneration, File file )
            {
                called.setTrue();
            }

            @Override
            public void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId, File file )
            {
                called.setTrue();
            }

            @Override
            public void pageIdSeenMultipleTimes( long pageId, File file )
            {
                called.setTrue();
            }

            @Override
            public void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId, File file )
            {
                called.setTrue();
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportCircularChildPointer( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId, File file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        } );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportException( GBPTree<KEY,VALUE> index ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<KEY>()
        {
            @Override
            public void exception( Exception e )
            {
                called.setTrue();
            }
        } );
        assertCalled( called );
    }

    private static void assertCalled( MutableBoolean called )
    {
        assertTrue( "Expected to receive call to correct consistency report method.", called.getValue() );
    }

    private long randomAmong( LongList list )
    {
        return list.get( random.nextInt( list.size() ) );
    }
}
