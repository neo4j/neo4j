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

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Math.toIntExact;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

/**
 * <ul>
 * Checks:
 * <li>order of keys in isolated nodes
 * <li>keys fit inside range given by parent node
 * <li>sibling pointers match
 * <li>GSPP
 * </ul>
 */
class GBPTreeConsistencyChecker<KEY>
{
    private final TreeNode<KEY,?> node;
    private final Comparator<KEY> comparator;
    private final Layout<KEY,?> layout;
    private final List<RightmostInChain> rightmostPerLevel = new ArrayList<>();
    private final IdProvider idProvider;
    private final long lastId;
    private final long stableGeneration;
    private final long unstableGeneration;
    private final GenerationKeeper generationTarget = new GenerationKeeper();

    GBPTreeConsistencyChecker( TreeNode<KEY,?> node, Layout<KEY,?> layout, IdProvider idProvider, long stableGeneration,
            long unstableGeneration )
    {
        this.node = node;
        this.comparator = node.keyComparator();
        this.layout = layout;
        this.idProvider = idProvider;
        this.lastId = idProvider.lastId();
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
    }

    public void check( PageCursor cursor, Root root, GBPTreeConsistencyCheckVisitor<KEY> visitor ) throws IOException
    {
        long highId = lastId + 1;
        BitSet seenIds = new BitSet( Math.toIntExact( highId ) );

        // Log ids in freelist together with ids occupied by freelist pages.
        IdProvider.IdProviderVisitor freelistSeenIdsVisitor = new FreelistSeenIdsVisitor<>( seenIds, lastId, visitor );
        idProvider.visitFreelist( freelistSeenIdsVisitor );

        // Check structure of GBPTree
        long rootGeneration = root.goTo( cursor );
        KeyRange<KEY> openRange = new KeyRange<>( -1, -1, comparator, null, null, layout, null );
        checkSubtree( cursor, openRange, -1, rootGeneration, GBPTreePointerType.noPointer(), 0, visitor, seenIds );

        // Assert that rightmost node on each level has empty right sibling.
        rightmostPerLevel.forEach( rightmost -> rightmost.assertLast( visitor ) );

        // Assert that all pages in file are either present as an active tree node or in freelist.
        assertAllIdsOccupied( highId, seenIds, visitor );
        root.goTo( cursor );
    }

    private static <KEY> void assertAllIdsOccupied( long highId, BitSet seenIds, GBPTreeConsistencyCheckVisitor<KEY> visitor )
    {
        long expectedNumberOfPages = highId - IdSpace.MIN_TREE_NODE_ID;
        if ( seenIds.cardinality() != expectedNumberOfPages )
        {
            int index = (int) IdSpace.MIN_TREE_NODE_ID;
            while ( index >= 0 && index < highId )
            {
                index = seenIds.nextClearBit( index );
                if ( index != -1 && index < highId )
                {
                    visitor.unusedPage( index );
                }
                index++;
            }
        }
    }

    private static <KEY> void addToSeenList( BitSet target, long id, long lastId, GBPTreeConsistencyCheckVisitor<KEY> visitor )
    {
        int index = toIntExact( id );
        if ( target.get( index ) )
        {
            visitor.pageIdSeenMultipleTimes( id );
        }
        if ( id > lastId )
        {
            throw new IllegalStateException( "Unexpectedly high id " + id + " seen when last id is " + lastId );
        }
        target.set( index );
    }

    private void checkSubtree( PageCursor cursor, KeyRange<KEY> range, long parentNode, long pointerGeneration, GBPTreePointerType parentPointerType, int level,
            GBPTreeConsistencyCheckVisitor<KEY> visitor, BitSet seenIds ) throws IOException
    {
        long pageId = cursor.getCurrentPageId();
        addToSeenList( seenIds, pageId, lastId, visitor );
        if ( range.hasPageIdInStack( pageId ) )
        {
            visitor.childNodeFoundAmongParentNodes( level, pageId, range );
            return;
        }
        byte nodeType;
        byte treeNodeType;
        int keyCount;
        long successor;

        long leftSiblingPointer;
        long rightSiblingPointer;
        long leftSiblingPointerGeneration;
        long rightSiblingPointerGeneration;
        long currentNodeGeneration;

        do
        {
            // for assertSiblings
            leftSiblingPointer = TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration, generationTarget );
            leftSiblingPointerGeneration = generationTarget.generation;
            rightSiblingPointer = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration, generationTarget );
            rightSiblingPointerGeneration = generationTarget.generation;
            leftSiblingPointer = pointer( leftSiblingPointer );
            rightSiblingPointer = pointer( rightSiblingPointer );
            currentNodeGeneration = TreeNode.generation( cursor );

            successor = TreeNode.successor( cursor, stableGeneration, unstableGeneration, generationTarget );

            keyCount = TreeNode.keyCount( cursor );
            nodeType = TreeNode.nodeType( cursor );
            treeNodeType = TreeNode.treeNodeType( cursor );
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE )
        {
             visitor.notATreeNode( pageId );
             return;
        }

        boolean isLeaf = treeNodeType == TreeNode.LEAF_FLAG;
        boolean isInternal = treeNodeType == TreeNode.INTERNAL_FLAG;
        if ( !isInternal && !isLeaf )
        {
            visitor.unknownTreeNodeType( pageId, treeNodeType );
            return;
        }

        // check header pointers
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, GBPTreePointerType.leftSibling(), TreeNode.BYTE_POS_LEFTSIBLING, visitor );
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, GBPTreePointerType.rightSibling(), TreeNode.BYTE_POS_RIGHTSIBLING, visitor );
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, GBPTreePointerType.successor(), TreeNode.BYTE_POS_SUCCESSOR, visitor );

        boolean reasonableKeyCount = node.reasonableKeyCount( keyCount );
        if ( !reasonableKeyCount )
        {
            visitor.unreasonableKeyCount( pageId, keyCount );
        }
        else
        {
            assertKeyOrder( cursor, range, keyCount, isLeaf ? LEAF : INTERNAL, visitor );
        }

        do
        {
            // todo place this report outside of shouldRetry
            node.checkMetaConsistency( cursor, keyCount, isLeaf ? LEAF : INTERNAL, visitor );
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        assertPointerGenerationMatchesGeneration( parentPointerType, parentNode, pageId, pointerGeneration,
                currentNodeGeneration, visitor );
        assertSiblings( cursor, currentNodeGeneration, leftSiblingPointer, leftSiblingPointerGeneration, rightSiblingPointer,
                rightSiblingPointerGeneration, level, visitor );
        checkSuccessorPointerGeneration( cursor, successor, visitor );

        if ( isInternal && reasonableKeyCount )
        {
            assertSubtrees( cursor, range, keyCount, level, visitor, seenIds );
        }
    }

    private static <KEY> void assertPointerGenerationMatchesGeneration( GBPTreePointerType pointerType, long sourceNode, long pointer, long pointerGeneration,
            long targetNodeGeneration, GBPTreeConsistencyCheckVisitor<KEY> visitor )
    {
        if ( targetNodeGeneration > pointerGeneration )
        {
            visitor.pointerHasLowerGenerationThanNode( pointerType, sourceNode, pointer, pointerGeneration, targetNodeGeneration );
        }
    }

    private void checkSuccessorPointerGeneration( PageCursor cursor, long successor, GBPTreeConsistencyCheckVisitor<KEY> visitor )
    {
        if ( TreeNode.isNode( successor ) )
        {
            visitor.pointerToOldVersionOfTreeNode( cursor.getCurrentPageId(), pointer( successor ) );
        }
    }

    // Assumption: We traverse the tree from left to right on every level
    private void assertSiblings( PageCursor cursor, long currentNodeGeneration, long leftSiblingPointer,
            long leftSiblingPointerGeneration, long rightSiblingPointer, long rightSiblingPointerGeneration, int level,
            GBPTreeConsistencyCheckVisitor<KEY> visitor )
    {
        // If this is the first time on this level, we will add a new entry
        for ( int i = rightmostPerLevel.size(); i <= level; i++ )
        {
            rightmostPerLevel.add( i, new RightmostInChain() );
        }
        RightmostInChain rightmost = rightmostPerLevel.get( level );

        rightmost.assertNext( cursor, currentNodeGeneration, leftSiblingPointer, leftSiblingPointerGeneration, rightSiblingPointer,
                rightSiblingPointerGeneration, visitor );
    }

    private void assertSubtrees( PageCursor cursor, KeyRange<KEY> range, int keyCount, int level,
            GBPTreeConsistencyCheckVisitor<KEY> visitor, BitSet seenIds ) throws IOException
    {
        long pageId = cursor.getCurrentPageId();
        KEY prev = null;
        KeyRange<KEY> childRange;
        KEY readKey = layout.newKey();

        // Check children, all except the last one
        int pos = 0;
        while ( pos < keyCount )
        {
            long child;
            long childGeneration;
            assertNoCrashOrBrokenPointerInGSPP(
                    cursor, stableGeneration, unstableGeneration, GBPTreePointerType.child( pos ), node.childOffset( pos ), visitor );
            do
            {
                child = childAt( cursor, pos, generationTarget );
                childGeneration = generationTarget.generation;
                node.keyAt( cursor, readKey, pos, INTERNAL );
            }
            while ( cursor.shouldRetry() );
            checkAfterShouldRetry( cursor );

            childRange = range.newSubRange( level, pageId ).restrictRight( readKey );
            if ( pos > 0 )
            {
                childRange = childRange.restrictLeft( prev );
            }

            TreeNode.goTo( cursor, "child at pos " + pos, child );
            checkSubtree( cursor, childRange, pageId, childGeneration, GBPTreePointerType.child( pos ), level + 1, visitor, seenIds );

            TreeNode.goTo( cursor, "parent", pageId );

            if ( pos == 0 )
            {
                prev = layout.newKey();
            }
            layout.copyKey( readKey, prev );
            pos++;
        }

        // Check last child
        long child;
        long childGeneration;
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, GBPTreePointerType.child( pos ), node.childOffset( pos ), visitor );
        do
        {
            child = childAt( cursor, pos, generationTarget );
            childGeneration = generationTarget.generation;
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        TreeNode.goTo( cursor, "child at pos " + pos, child );
        childRange = range.newSubRange( level, pageId ).restrictLeft( prev );
        checkSubtree( cursor, childRange, pageId, childGeneration, GBPTreePointerType.child( pos ), level + 1, visitor, seenIds );
        TreeNode.goTo( cursor, "parent", pageId );
    }

    private static void checkAfterShouldRetry( PageCursor cursor ) throws CursorException
    {
        checkOutOfBounds( cursor );
        cursor.checkAndClearCursorException();
    }

    private long childAt( PageCursor cursor, int pos, GBPTreeGenerationTarget childGeneration )
    {
        return node.childAt( cursor, pos, stableGeneration, unstableGeneration, childGeneration );
    }

    private void assertKeyOrder( PageCursor cursor, KeyRange<KEY> range, int keyCount, TreeNode.Type type,
            GBPTreeConsistencyCheckVisitor<KEY> visitor ) throws IOException
    {
        DelayedVisitor<KEY> delayedVisitor = new DelayedVisitor<>();
        do
        {
            delayedVisitor.clear();
            KEY prev = layout.newKey();
            KEY readKey = layout.newKey();
            boolean first = true;
            for ( int pos = 0; pos < keyCount; pos++ )
            {
                node.keyAt( cursor, readKey, pos, type );
                if ( !range.inRange( readKey ) )
                {
                    KEY keyCopy = layout.newKey();
                    layout.copyKey( readKey, keyCopy );
                    delayedVisitor.keysLocatedInWrongNode( cursor.getCurrentPageId(), range, keyCopy, pos, keyCount );
                }
                if ( !first )
                {
                    if ( comparator.compare( prev, readKey ) >= 0 )
                    {
                        delayedVisitor.keysOutOfOrderInNode( cursor.getCurrentPageId() );
                    }
                }
                else
                {
                    first = false;
                }
                layout.copyKey( readKey, prev );
            }
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );
        delayedVisitor.report( visitor );
    }

    static <KEY> void assertNoCrashOrBrokenPointerInGSPP( PageCursor cursor, long stableGeneration, long unstableGeneration,
            GBPTreePointerType pointerType, int offset, GBPTreeConsistencyCheckVisitor<KEY> visitor ) throws IOException
    {
        long currentNodeId = cursor.getCurrentPageId();

        long generationA;
        long readPointerA;
        long pointerA;
        short checksumA;
        boolean correctChecksumA;
        byte stateA;

        long generationB;
        long readPointerB;
        long pointerB;
        short checksumB;
        boolean correctChecksumB;
        byte stateB;
        do
        {
            cursor.setOffset( offset );
            // A
            generationA = GenerationSafePointer.readGeneration( cursor );
            readPointerA = GenerationSafePointer.readPointer( cursor );
            pointerA = GenerationSafePointerPair.pointer( readPointerA );
            checksumA = GenerationSafePointer.readChecksum( cursor );
            correctChecksumA = GenerationSafePointer.checksumOf( generationA, readPointerA ) == checksumA;
            stateA = GenerationSafePointerPair.pointerState(
                    stableGeneration, unstableGeneration, generationA, readPointerA, correctChecksumA );

            // B
            generationB = GenerationSafePointer.readGeneration( cursor );
            readPointerB = GenerationSafePointer.readPointer( cursor );
            pointerB = GenerationSafePointerPair.pointer( readPointerA );
            checksumB = GenerationSafePointer.readChecksum( cursor );
            correctChecksumB = GenerationSafePointer.checksumOf( generationB, readPointerB ) == checksumB;
            stateB = GenerationSafePointerPair.pointerState(
                    stableGeneration, unstableGeneration, generationB, readPointerB, correctChecksumB );
        }
        while ( cursor.shouldRetry() );

        if ( stateA == GenerationSafePointerPair.CRASH || stateB == GenerationSafePointerPair.CRASH )
        {
            visitor.crashedPointer( currentNodeId, pointerType,
                    generationA, readPointerA, pointerA, stateA,
                    generationB, readPointerB, pointerB, stateB );
        }
        if ( stateA == GenerationSafePointerPair.BROKEN || stateB == GenerationSafePointerPair.BROKEN )
        {
            visitor.brokenPointer( currentNodeId, pointerType,
                    generationA, readPointerA, pointerA, stateA,
                    generationB, readPointerB, pointerB, stateB );
        }
    }

    private static class DelayedVisitor<KEY> extends GBPTreeConsistencyCheckVisitor.Adaptor<KEY>
    {
        MutableLongList keysOutOfOrder = LongLists.mutable.empty();
        MutableList<KeyInWrongNode<KEY>> keysLocatedInWrongNode = Lists.mutable.empty();

        @Override
        public void keysOutOfOrderInNode( long pageId )
        {
            keysOutOfOrder.add( pageId );
        }

        @Override
        public void keysLocatedInWrongNode( long pageId, KeyRange<KEY> range, KEY key, int pos, int keyCount )
        {
            keysLocatedInWrongNode.add( new KeyInWrongNode<>( pageId, range, key, pos, keyCount ) );
        }

        void clear()
        {
            keysOutOfOrder.clear();
            keysLocatedInWrongNode.clear();
        }

        void report( GBPTreeConsistencyCheckVisitor<KEY> visitor )
        {
            if ( keysOutOfOrder.notEmpty() )
            {
                keysOutOfOrder.forEach( visitor::keysOutOfOrderInNode );
            }
            if ( keysLocatedInWrongNode.notEmpty() )
            {
                keysLocatedInWrongNode.forEach( keyInWrongNode -> visitor.keysLocatedInWrongNode(
                        keyInWrongNode.pageId, keyInWrongNode.range, keyInWrongNode.key, keyInWrongNode.pos, keyInWrongNode.keyCount ) );
            }
        }

        private static class KeyInWrongNode<KEY>
        {
            final long pageId;
            final KeyRange<KEY> range;
            final KEY key;
            final int pos;
            final int keyCount;

            private KeyInWrongNode( long pageId, KeyRange<KEY> range, KEY key, int pos, int keyCount )
            {
                this.pageId = pageId;
                this.range = range;
                this.key = key;
                this.pos = pos;
                this.keyCount = keyCount;
            }
        }
    }

    private static class FreelistSeenIdsVisitor<KEY> implements IdProvider.IdProviderVisitor
    {
        private final BitSet seenIds;
        private final long lastId;
        private final GBPTreeConsistencyCheckVisitor<KEY> visitor;

        private FreelistSeenIdsVisitor( BitSet seenIds, long lastId, GBPTreeConsistencyCheckVisitor<KEY> visitor )
        {
            this.seenIds = seenIds;
            this.lastId = lastId;
            this.visitor = visitor;
        }

        @Override
        public void beginFreelistPage( long pageId )
        {
            addToSeenList( seenIds, pageId, lastId, visitor );
        }

        @Override
        public void endFreelistPage( long pageId )
        {
        }

        @Override
        public void freelistEntry( long pageId, long generation, int pos )
        {
            addToSeenList( seenIds, pageId, lastId, visitor );
        }
    }
}
