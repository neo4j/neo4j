/*
 * Copyright (c) "Neo4j"
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.GBPTreeOpenOptions.NO_FLUSH_ON_CLOSE;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.test.utils.PageCacheConfig.config;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectorySupportExtension.class, RandomExtension.class} )
abstract class GBPTreeConsistencyCheckerTestBase<KEY,VALUE>
{
    private static final int PAGE_SIZE = 512;

    @RegisterExtension
    static final PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();

    @Inject
    EphemeralFileSystemAbstraction fs;
    @Inject
    TestDirectory directory;
    @Inject
    RandomSupport random;

    TestLayout<KEY,VALUE> layout;
    private RandomValues randomValues;
    private Path indexFile;
    private PageCache pageCache;
    private boolean isDynamic;

    @BeforeEach
    void setUp()
    {
        indexFile = directory.file( "index" );
        pageCache = createPageCache();
        layout = getLayout();
        randomValues = random.randomValues();
        isDynamic = !layout.fixedSize();
    }

    @AfterEach
    void tearDown()
    {
        pageCache.close();
    }

    protected abstract TestLayout<KEY,VALUE> getLayout();

    private PageCache createPageCache()
    {
        return PageCacheSupportExtension.getPageCache( fs, config().withPageSize( PAGE_SIZE ) );
    }

    @Test
    void shouldDetectNotATreeNodeRoot() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long rootNode = inspection.rootNode();

            index.unsafe( page( rootNode, GBPTreeCorruption.notATreeNode() ), NULL_CONTEXT );

            assertReportNotATreeNode( index, rootNode );
        }
    }

    @Test
    void shouldDetectNotATreeNodeInternal() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            final ImmutableLongList internalNodes = inspection.internalNodes();
            long internalNode = randomAmong( internalNodes );

            index.unsafe( page( internalNode, GBPTreeCorruption.notATreeNode() ), NULL_CONTEXT );

            assertReportNotATreeNode( index, internalNode );
        }
    }

    @Test
    void shouldDetectNotATreeNodeLeaf() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long leafNode = randomAmong( inspection.leafNodes() );

            index.unsafe( page( leafNode, GBPTreeCorruption.notATreeNode() ), NULL_CONTEXT );

            assertReportNotATreeNode( index, leafNode );
        }
    }

    @Test
    void shouldDetectUnknownTreeNodeTypeRoot() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long rootNode = inspection.rootNode();

            index.unsafe( page( rootNode, GBPTreeCorruption.unknownTreeNodeType() ), NULL_CONTEXT );

            assertReportUnknownTreeNodeType( index, rootNode );
        }
    }

    @Test
    void shouldDetectUnknownTreeNodeTypeInternal() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long internalNode = randomAmong( inspection.internalNodes() );

            index.unsafe( page( internalNode, GBPTreeCorruption.unknownTreeNodeType() ), NULL_CONTEXT );

            assertReportUnknownTreeNodeType( index, internalNode );
        }
    }

    @Test
    void shouldDetectUnknownTreeNodeTypeLeaf() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long leafNode = randomAmong( inspection.leafNodes() );

            index.unsafe( page( leafNode, GBPTreeCorruption.unknownTreeNodeType() ), NULL_CONTEXT );

            assertReportUnknownTreeNodeType( index, leafNode );
        }
    }

    @Test
    void shouldDetectRightSiblingNotPointingToCorrectSibling() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.leafNodes() );

            index.unsafe( page( targetNode, GBPTreeCorruption.rightSiblingPointToNonExisting() ), NULL_CONTEXT );

            assertReportMisalignedSiblingPointers( index );
        }
    }

    @Test
    void shouldDetectLeftSiblingNotPointingToCorrectSibling() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.leafNodes() );

            index.unsafe( page( targetNode, GBPTreeCorruption.leftSiblingPointToNonExisting() ), NULL_CONTEXT );

            assertReportMisalignedSiblingPointers( index );
        }
    }

    @Test
    void shouldDetectIfAnyNodeInTreeHasSuccessor() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.allNodes() );

            index.unsafe( page( targetNode, GBPTreeCorruption.hasSuccessor() ), NULL_CONTEXT );

            assertReportPointerToOldVersionOfTreeNode( index, targetNode );
        }
    }

    @Test
    void shouldDetectRightSiblingPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = nodeWithRightSibling( inspection );

            index.unsafe( page( targetNode, GBPTreeCorruption.rightSiblingPointerHasTooLowGeneration() ), NULL_CONTEXT );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.rightSibling() );
        }
    }

    @Test
    void shouldDetectLeftSiblingPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = nodeWithLeftSibling( inspection );

            index.unsafe( page( targetNode, GBPTreeCorruption.leftSiblingPointerHasTooLowGeneration() ), NULL_CONTEXT );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.leftSibling() );
        }
    }

    @Test
    void shouldDetectChildPointerWithTooLowGeneration() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.internalNodes() );
            int keyCount = inspection.keyCounts().get( targetNode );
            int childPos = randomValues.nextInt( keyCount + 1 );

            index.unsafe( page( targetNode, GBPTreeCorruption.childPointerHasTooLowGeneration( childPos ) ), NULL_CONTEXT );

            assertReportPointerGenerationLowerThanNodeGeneration( index, targetNode, GBPTreePointerType.child( childPos ) );
        }
    }

    @Test
    void shouldDetectKeysOutOfOrderInIsolatedNode() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = nodeWithMultipleKeys( inspection );
            int keyCount = inspection.keyCounts().get( targetNode );
            int firstKey = randomValues.nextInt( keyCount );
            int secondKey = nextRandomIntExcluding( keyCount, firstKey );
            boolean isLeaf = inspection.leafNodes().contains( targetNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> swapKeyOrder = isLeaf ?
                                                                       GBPTreeCorruption.swapKeyOrderLeaf( firstKey, secondKey, keyCount ) :
                                                                       GBPTreeCorruption.swapKeyOrderInternal( firstKey, secondKey, keyCount );
            index.unsafe( page( targetNode, swapKeyOrder ), NULL_CONTEXT );

            assertReportKeysOutOfOrderInNode( index, targetNode );
        }
    }

    @Test
    void shouldDetectKeysLocatedInWrongNodeLowKey() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = nodeWithLeftSibling( inspection );
            int keyCount = inspection.keyCounts().get( targetNode );
            int keyPos = randomValues.nextInt( keyCount );
            KEY key = layout.key( Long.MIN_VALUE );
            boolean isLeaf = inspection.leafNodes().contains( targetNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> swapKeyOrder = isLeaf ?
                                                                       GBPTreeCorruption.overwriteKeyAtPosLeaf( key, keyPos, keyCount ) :
                                                                       GBPTreeCorruption.overwriteKeyAtPosInternal( key, keyPos, keyCount );
            index.unsafe( page( targetNode, swapKeyOrder ), NULL_CONTEXT );

            assertReportKeysLocatedInWrongNode( index, targetNode );
        }
    }

    @Test
    void shouldDetectKeysLocatedInWrongNodeHighKey() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = nodeWithRightSibling( inspection );
            int keyCount = inspection.keyCounts().get( targetNode );
            int keyPos = randomValues.nextInt( keyCount );
            KEY key = layout.key( Long.MAX_VALUE );
            boolean isLeaf = inspection.leafNodes().contains( targetNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> swapKeyOrder = isLeaf ?
                                                                       GBPTreeCorruption.overwriteKeyAtPosLeaf( key, keyPos, keyCount ) :
                                                                       GBPTreeCorruption.overwriteKeyAtPosInternal( key, keyPos, keyCount );
            index.unsafe( page( targetNode, swapKeyOrder ), NULL_CONTEXT );

            assertReportKeysLocatedInWrongNode( index, targetNode );
        }
    }

    @Test
    void shouldDetectNodeMetaInconsistencyDynamicNodeAllocSpaceOverlapActiveKeys() throws IOException
    {
        Assumptions.assumeTrue( isDynamic, "Only relevant for dynamic layout" );
        try ( GBPTree<KEY,VALUE> index = index( layout ).build() )
        {
            treeWithHeight( index, layout, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.allNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.maximizeAllocOffsetInDynamicNode();
            index.unsafe( page( targetNode, corruption ), NULL_CONTEXT );

            assertReportAllocSpaceOverlapActiveKeys( index, targetNode );
        }
    }

    @Test
    void shouldDetectNodeMetaInconsistencyDynamicNodeOverlapBetweenOffsetArrayAndAllocSpace() throws IOException
    {
        Assumptions.assumeTrue( isDynamic, "Only relevant for dynamic layout" );
        try ( GBPTree<KEY,VALUE> index = index( layout ).build() )
        {
            treeWithHeight( index, layout, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.allNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.minimizeAllocOffsetInDynamicNode();
            index.unsafe( page( targetNode, corruption ), NULL_CONTEXT );

            assertReportAllocSpaceOverlapOffsetArray( index, targetNode );
        }
    }

    @Test
    void shouldDetectNodeMetaInconsistencyDynamicNodeSpaceAreasNotSummingToTotalSpace() throws IOException
    {
        Assumptions.assumeTrue( isDynamic, "Only relevant for dynamic layout" );
        try ( GBPTree<KEY,VALUE> index = index( layout ).build() )
        {
            treeWithHeight( index, layout, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.allNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.incrementDeadSpaceInDynamicNode();
            index.unsafe( page( targetNode, corruption ), NULL_CONTEXT );

            assertReportSpaceAreasNotSummingToTotalSpace( index, targetNode );
        }
    }

    @Test
    void shouldDetectNodeMetaInconsistencyDynamicNodeAllocOffsetMisplaced() throws IOException
    {
        Assumptions.assumeTrue( isDynamic, "Only relevant for dynamic layout" );
        try ( GBPTree<KEY,VALUE> index = index( layout ).build() )
        {
            treeWithHeight( index, layout, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.allNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.decrementAllocOffsetInDynamicNode();
            index.unsafe( page( targetNode, corruption ), NULL_CONTEXT );

            assertReportAllocOffsetMisplaced( index, targetNode );
        }
    }

    @Test
    void shouldDetectPageMissingFreelistEntry() throws IOException
    {
        long targetMissingId;
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<KEY,VALUE> writer = index.writer( NULL_CONTEXT ) )
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
            index.checkpoint( NULL_CONTEXT );
        }

        // When tree is closed we will overwrite treeState with in memory state so we need to open tree in special mode for our state corruption to persist.
        try ( GBPTree<KEY,VALUE> index = index().with( immutable.with( NO_FLUSH_ON_CLOSE ) ).build() )
        {
            GBPTreeInspection inspection = inspect( index );
            int lastIndex = inspection.allFreelistEntries().size() - 1;
            FreelistEntry lastFreelistEntry = inspection.allFreelistEntries().get( lastIndex );
            targetMissingId = lastFreelistEntry.id;

            GBPTreeCorruption.IndexCorruption<KEY,VALUE> corruption = GBPTreeCorruption.decrementFreelistWritePos();
            index.unsafe( corruption, NULL_CONTEXT );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            assertReportUnusedPage( index, targetMissingId );
        }
    }

    @Test
    void shouldDetectExtraFreelistEntry() throws IOException
    {
        long targetNode;
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<KEY,VALUE> writer = index.writer( NULL_CONTEXT ) )
            {
                int keyCount = 0;
                while ( getHeight( index ) < 2 )
                {
                    writer.put( layout.key( keyCount ), layout.value( keyCount ) );
                    keyCount++;
                }
            }
            index.checkpoint( NULL_CONTEXT );
        }

        // When tree is closed we will overwrite treeState with in memory state so we need to open tree in special mode for our state corruption to persist.
        try ( GBPTree<KEY,VALUE> index = index().with( immutable.with( NO_FLUSH_ON_CLOSE ) ).build() )
        {
            GBPTreeInspection inspection = inspect( index );
            targetNode = randomAmong( inspection.allNodes() );

            GBPTreeCorruption.IndexCorruption<KEY,VALUE> corruption = GBPTreeCorruption.addFreelistEntry( targetNode );
            index.unsafe( corruption, NULL_CONTEXT );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            assertReportActiveTreeNodeInFreelist( index, targetNode );
        }
    }

    @Test
    void shouldDetectExtraEmptyPageInFile() throws IOException
    {
        long lastId;
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<KEY,VALUE> writer = index.writer( NULL_CONTEXT ) )
            {
                int keyCount = 0;
                while ( getHeight( index ) < 2 )
                {
                    writer.put( layout.key( keyCount ), layout.value( keyCount ) );
                    keyCount++;
                }
            }
            index.checkpoint( NULL_CONTEXT );
        }

        // When tree is closed we will overwrite treeState with in memory state so we need to open tree in special mode for our state corruption to persist.
        try ( GBPTree<KEY,VALUE> index = index().with( immutable.with( NO_FLUSH_ON_CLOSE ) ).build() )
        {
            GBPTreeInspection inspection = inspect( index );
            TreeState treeState = inspection.treeState();
            lastId = treeState.lastId() + 1;
            TreeState newTreeState = treeStateWithLastId( lastId, treeState );

            GBPTreeCorruption.IndexCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setTreeState( newTreeState );
            index.unsafe( corruption, NULL_CONTEXT );
        }

        // Need to restart tree to reload corrupted freelist
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            assertReportUnusedPage( index, lastId );
        }
    }

    @Test
    void shouldDetectIdLargerThanFreelistLastId() throws IOException
    {
        long targetLastId;
        long targetPageId;
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            // Add and remove a bunch of keys to fill freelist
            try ( Writer<KEY,VALUE> writer = index.writer( NULL_CONTEXT ) )
            {
                int keyCount = 0;
                while ( getHeight( index ) < 2 )
                {
                    writer.put( layout.key( keyCount ), layout.value( keyCount ) );
                    keyCount++;
                }
            }
            index.checkpoint( NULL_CONTEXT );
        }

        // When tree is closed we will overwrite treeState with in memory state so we need to open tree in special mode for our state corruption to persist.
        try ( GBPTree<KEY,VALUE> index = index().with( immutable.with( NO_FLUSH_ON_CLOSE ) ).build() )
        {
            GBPTreeInspection inspection = inspect( index );
            TreeState treeState = inspection.treeState();
            targetPageId = treeState.lastId();
            targetLastId = treeState.lastId() - 1;
            TreeState newTreeState = treeStateWithLastId( targetLastId, treeState );

            GBPTreeCorruption.IndexCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setTreeState( newTreeState );
            index.unsafe( corruption, NULL_CONTEXT );
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
    void shouldDetectCrashedGSPP() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.allNodes() );
            boolean isLeaf = inspection.leafNodes().contains( targetNode );
            int keyCount = inspection.keyCounts().get( targetNode );
            GBPTreePointerType pointerType = randomPointerType( keyCount, isLeaf );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.crashed( pointerType );
            index.unsafe( page( targetNode, corruption ), NULL_CONTEXT );

            assertReportCrashedGSPP( index, targetNode, pointerType );
        }
    }

    @Test
    void shouldDetectBrokenGSPP() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.allNodes() );
            boolean isLeaf = inspection.leafNodes().contains( targetNode );
            int keyCount = inspection.keyCounts().get( targetNode );
            GBPTreePointerType pointerType = randomPointerType( keyCount, isLeaf );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.broken( pointerType );
            index.unsafe( page( targetNode, corruption ), NULL_CONTEXT );

            assertReportBrokenGSPP( index, targetNode, pointerType );
        }
    }

    @Test
    void shouldDetectUnreasonableKeyCount() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long targetNode = randomAmong( inspection.allNodes() );
            int unreasonableKeyCount = PAGE_SIZE;

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setKeyCount( unreasonableKeyCount );
            index.unsafe( page( targetNode, corruption ), NULL_CONTEXT );

            assertReportUnreasonableKeyCount( index, targetNode, unreasonableKeyCount );
        }
    }

    @Test
    void shouldDetectChildPointerPointingTwoLevelsDown() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long rootNode = inspection.rootNode();
            int childPos = randomChildPos( inspection, rootNode );
            long targetChildNode = randomAmong( inspection.leafNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setChild( childPos, targetChildNode );
            index.unsafe( page( rootNode, corruption ), NULL_CONTEXT );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    void shouldDetectChildPointerPointingToUpperLevelSameStack() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long rootNode = inspection.rootNode();
            long internalNode = randomAmong( inspection.nodesPerLevel().get( 1 ) );
            int childPos = randomChildPos( inspection, internalNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setChild( childPos, rootNode );
            index.unsafe( page( internalNode, corruption ), NULL_CONTEXT );

            assertReportCircularChildPointer( index, rootNode );
        }
    }

    @Test
    void shouldDetectChildPointerPointingToSameLevel() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            ImmutableLongList internalNodesWithSiblings = inspection.nodesPerLevel().get( 1 );
            long internalNode = randomAmong( internalNodesWithSiblings );
            long otherInternalNode = randomFromExcluding( internalNodesWithSiblings, internalNode );
            int childPos = randomChildPos( inspection, internalNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setChild( childPos, otherInternalNode );
            index.unsafe( page( internalNode, corruption ), NULL_CONTEXT );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    void shouldDetectChildPointerPointingToUpperLevelNotSameStack() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 3 );

            GBPTreeInspection inspection = inspect( index );
            long upperInternalNode = randomAmong( inspection.nodesPerLevel().get( 1 ) );
            long lowerInternalNode = randomAmong( inspection.nodesPerLevel().get( 2 ) );
            int childPos = randomChildPos( inspection, lowerInternalNode );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setChild( childPos, upperInternalNode );
            index.unsafe( page( lowerInternalNode, corruption ), NULL_CONTEXT );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    void shouldDetectChildPointerPointingToChildOwnedByOtherNode() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            LongList internalNodesWithSiblings = inspection.nodesPerLevel().get( 1 );
            long targetInternalNode = randomAmong( internalNodesWithSiblings );
            long otherInternalNode = randomFromExcluding( internalNodesWithSiblings, targetInternalNode );
            int otherChildPos = randomChildPos( inspection, otherInternalNode );
            int targetChildPos = randomChildPos( inspection, targetInternalNode );

            GBPTreeCorruption.IndexCorruption<KEY,VALUE> corruption =
                    GBPTreeCorruption.copyChildPointerFromOther( targetInternalNode, otherInternalNode, targetChildPos, otherChildPos );
            index.unsafe( corruption, NULL_CONTEXT );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    void shouldDetectExceptionDuringConsistencyCheck() throws IOException
    {
        Assumptions.assumeTrue( isDynamic, "This trick to make GBPTreeConsistencyChecker throw exception only work for dynamic layout" );
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            final long leaf = randomAmong( inspection.leafNodes() );

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setHighestReasonableKeyCount();
            index.unsafe( page( leaf, corruption ), NULL_CONTEXT );

            assertReportException( index );
        }
    }

    @Test
    void shouldDetectSiblingPointerPointingToLowerLevel() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long internalNode = randomAmong( inspection.internalNodes() );
            long leafNode = randomAmong( inspection.leafNodes() );
            GBPTreePointerType siblingPointer = randomSiblingPointerType();

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setPointer( siblingPointer, leafNode );
            index.unsafe( page( internalNode, corruption ), NULL_CONTEXT );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    void shouldDetectSiblingPointerPointingToUpperLevel() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            treeWithHeight( index, 2 );

            GBPTreeInspection inspection = inspect( index );
            long internalNode = randomAmong( inspection.internalNodes() );
            long leafNode = randomAmong( inspection.leafNodes() );
            GBPTreePointerType siblingPointer = randomSiblingPointerType();

            GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption = GBPTreeCorruption.setPointer( siblingPointer, internalNode );
            index.unsafe( page( leafNode, corruption ), NULL_CONTEXT );

            assertReportAnyStructuralInconsistency( index );
        }
    }

    @Test
    void shouldDetectDirtyOnStartup() throws IOException
    {
        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            index.writer( NULL_CONTEXT ).close();
            // No checkpoint
        }

        try ( GBPTree<KEY,VALUE> index = index().build() )
        {
            assertReportDirtyOnStartup( index );
        }
    }

    private static <KEY, VALUE> GBPTreeCorruption.IndexCorruption<KEY,VALUE> page( long targetNode, GBPTreeCorruption.PageCorruption<KEY,VALUE> corruption )
    {
        return GBPTreeCorruption.pageSpecificCorruption( targetNode, corruption );
    }

    private long nodeWithLeftSibling( GBPTreeInspection visitor )
    {
        List<ImmutableLongList> nodesPerLevel = visitor.nodesPerLevel();
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

    private long nodeWithRightSibling( GBPTreeInspection inspection )
    {
        List<ImmutableLongList> nodesPerLevel = inspection.nodesPerLevel();
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

    private long nodeWithMultipleKeys( GBPTreeInspection inspection )
    {
        long targetNode;
        int keyCount;
        do
        {
            targetNode = randomAmong( inspection.allNodes() );
            keyCount = inspection.keyCounts().get( targetNode );
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

    private int randomChildPos( GBPTreeInspection inspection, long internalNode )
    {
        int childCount = inspection.keyCounts().get( internalNode ) + 1;
        return randomValues.nextInt( childCount );
    }

    private GBPTreePointerType randomSiblingPointerType()
    {
        return randomValues.among( Arrays.asList( GBPTreePointerType.leftSibling(), GBPTreePointerType.rightSibling() ) );
    }

    GBPTreeBuilder<KEY,VALUE> index()
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
        return switch ( randomValues.nextInt( bound ) )
                {
                    case 0 -> GBPTreePointerType.leftSibling();
                    case 1 -> GBPTreePointerType.rightSibling();
                    case 2 -> GBPTreePointerType.successor();
                    case 3 -> GBPTreePointerType.child( randomValues.nextInt( keyCount + 1 ) );
                    default -> throw new IllegalStateException( "Unrecognized option" );
                };
    }

    private void treeWithHeight( GBPTree<KEY,VALUE> index, int height ) throws IOException
    {
        treeWithHeight( index, layout, height );
    }

    private static <KEY, VALUE> void treeWithHeight( GBPTree<KEY,VALUE> index, TestLayout<KEY,VALUE> layout, int height ) throws IOException
    {
        try ( Writer<KEY,VALUE> writer = index.writer( NULL_CONTEXT ) )
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
        GBPTreeInspection inspection = inspect( index );
        return inspection.lastLevel();
    }

    static <KEY, VALUE> GBPTreeInspection inspect( GBPTree<KEY,VALUE> index ) throws IOException
    {
        return index.visit( new InspectingVisitor<>(), NULL_CONTEXT ).get();
    }

    private static <KEY,VALUE> void assertReportNotATreeNode( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void notATreeNode( long pageId, Path file )
            {
                called.setTrue();
                assertEquals( pageId, targetNode );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportUnknownTreeNodeType( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void unknownTreeNodeType( long pageId, byte treeNodeType, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportMisalignedSiblingPointers( GBPTree<KEY,VALUE> index ) throws IOException
    {
        MutableBoolean corruptedSiblingPointerCalled = new MutableBoolean();
        MutableBoolean rightmostNodeHasRightSiblingCalled = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration,
                    long leftRightSiblingPointer, long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode,
                    long rightNodeGeneration, Path file )
            {
                corruptedSiblingPointerCalled.setTrue();
            }

            @Override
            public void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode, Path file )
            {
                rightmostNodeHasRightSiblingCalled.setTrue();
            }
        }, NULL_CONTEXT );
        assertTrue( corruptedSiblingPointerCalled.getValue() || rightmostNodeHasRightSiblingCalled.getValue() );
    }

    private static <KEY,VALUE> void assertReportPointerToOldVersionOfTreeNode( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void pointerToOldVersionOfTreeNode( long pageId, long successorPointer, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( GenerationSafePointer.MAX_POINTER, successorPointer );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportPointerGenerationLowerThanNodeGeneration( GBPTree<KEY,VALUE> index, long targetNode,
            GBPTreePointerType expectedPointerType ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer,
                    long targetNodeGeneration, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, sourceNode );
                assertEquals( expectedPointerType, pointerType );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportKeysOutOfOrderInNode( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void keysOutOfOrderInNode( long pageId, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportKeysLocatedInWrongNode( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        Set<Long> allNodesWithKeysLocatedInWrongNode = new HashSet<>();
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId, Path file )
            {
                called.setTrue();
                allNodesWithKeysLocatedInWrongNode.add( pageId );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
        assertTrue( allNodesWithKeysLocatedInWrongNode.contains( targetNode ) );
    }

    private static <KEY,VALUE> void assertReportAllocSpaceOverlapActiveKeys( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertThat( message ).contains( "Overlap between allocSpace and active keys" );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportAllocSpaceOverlapOffsetArray( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertThat( message ).contains( "Overlap between offsetArray and allocSpace" );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportSpaceAreasNotSummingToTotalSpace( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertThat( message ).contains( "Space areas did not sum to total space" );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportAllocOffsetMisplaced( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void nodeMetaInconsistency( long pageId, String message, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertThat( message ).contains( "Pointer to allocSpace is misplaced, it should point to start of key" );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportUnusedPage( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void unusedPage( long pageId, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportActiveTreeNodeInFreelist( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void pageIdSeenMultipleTimes( long pageId, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportIdExceedLastId( GBPTree<KEY,VALUE> index, long targetLastId, long targetPageId ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void pageIdExceedLastId( long lastId, long pageId, Path file )
            {
                called.setTrue();
                assertEquals( targetLastId, lastId );
                assertEquals( targetPageId, pageId );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportCrashedGSPP( GBPTree<KEY,VALUE> index, long targetNode, GBPTreePointerType targetPointerType )
            throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void crashedPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
                    long generationB, long readPointerB, long pointerB, byte stateB, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( targetPointerType, pointerType );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportBrokenGSPP( GBPTree<KEY,VALUE> index, long targetNode, GBPTreePointerType targetPointerType )
            throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void brokenPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
                    long generationB, long readPointerB, long pointerB, byte stateB, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( targetPointerType, pointerType );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportUnreasonableKeyCount( GBPTree<KEY,VALUE> index, long targetNode, int targetKeyCount ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void unreasonableKeyCount( long pageId, int keyCount, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
                assertEquals( targetKeyCount, keyCount );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportAnyStructuralInconsistency( GBPTree<KEY,VALUE> index ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode, Path file )
            {
                called.setTrue();
            }

            @Override
            public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration,
                    long leftRightSiblingPointer, long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode,
                    long rightNodeGeneration, Path file )
            {
                called.setTrue();
            }

            @Override
            public void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId, Path file )
            {
                called.setTrue();
            }

            @Override
            public void pageIdSeenMultipleTimes( long pageId, Path file )
            {
                called.setTrue();
            }

            @Override
            public void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId, Path file )
            {
                called.setTrue();
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportCircularChildPointer( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId, Path file )
            {
                called.setTrue();
                assertEquals( targetNode, pageId );
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportException( GBPTree<KEY,VALUE> index ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void exception( Exception e )
            {
                called.setTrue();
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    private static <KEY,VALUE> void assertReportDirtyOnStartup( GBPTree<KEY,VALUE> index ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void dirtyOnStartup( Path file )
            {
                called.setTrue();
            }
        }, NULL_CONTEXT );
        assertCalled( called );
    }

    static void assertCalled( MutableBoolean called )
    {
        assertTrue( called.getValue(), "Expected to receive call to correct consistency report method." );
    }

    private long randomAmong( LongList list )
    {
        return list.get( random.nextInt( list.size() ) );
    }
}
