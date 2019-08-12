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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import org.neo4j.index.internal.gbptree.GenerationSafePointerPair.GenerationTarget;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
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
    private final long stableGeneration;
    private final long unstableGeneration;
    private final GenerationKeeper generationTarget = new GenerationKeeper();

    GBPTreeConsistencyChecker( TreeNode<KEY,?> node, Layout<KEY,?> layout, long stableGeneration, long unstableGeneration )
    {
        this.node = node;
        this.comparator = node.keyComparator();
        this.layout = layout;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
    }

    public void check( PageCursor cursor, Root root, GBPTreeConsistencyCheckVisitor<KEY> visitor ) throws IOException
    {
        long rootGeneration = root.goTo( cursor );
        KeyRange<KEY> openRange = new KeyRange<>( comparator, null, null, layout, null );
        checkSubtree( cursor, openRange, -1, rootGeneration, GBPTreePointerType.noPointer(), 0, visitor );

        // Assert that rightmost node on each level has empty right sibling.
        rightmostPerLevel.forEach( rightmost -> rightmost.assertLast( visitor ) );
        root.goTo( cursor );
    }

    /**
     * Checks so that all pages between {@link IdSpace#MIN_TREE_NODE_ID} and highest allocated id
     * are either in use in the tree, on the free-list or free-list nodes.
     *
     * @param cursor {@link PageCursor} to use for reading.
     * @param lastId highest allocated id in the store.
     * @param freelistIds page ids making up free-list pages and page ids on the free-list.
     * @return {@code true} if all pages are taken, otherwise {@code false}. Also is compatible with java
     * assert calls.
     * @throws IOException on {@link PageCursor} error.
     */
    boolean checkSpace( PageCursor cursor, long lastId, LongIterator freelistIds ) throws IOException
    {
        // TODO: limitation, can't run on an index larger than Integer.MAX_VALUE pages (which is fairly large)
        long highId = lastId + 1;
        BitSet seenIds = new BitSet( toIntExact( highId ) );
        while ( freelistIds.hasNext() )
        {
            addToSeenList( seenIds, freelistIds.next(), lastId );
        }

        // Traverse the tree
        do
        {
            // One level at the time
            long leftmostSibling = cursor.getCurrentPageId();
            addToSeenList( seenIds, leftmostSibling, lastId );

            // Go right through all siblings
            traverseAndAddRightSiblings( cursor, seenIds, lastId );

            // Then go back to the left-most node on this level
            TreeNode.goTo( cursor, "back", leftmostSibling );
        }
        // And continue down to next level if this level was an internal level
        while ( goToLeftmostChild( cursor ) );

        assertAllIdsOccupied( highId, seenIds );
        return true;
    }

    private boolean goToLeftmostChild( PageCursor cursor ) throws IOException
    {
        boolean isInternal;
        long leftmostSibling = -1;
        do
        {
            isInternal = TreeNode.isInternal( cursor );
            if ( isInternal )
            {
                leftmostSibling = node.childAt( cursor, 0, stableGeneration, unstableGeneration );
            }
        }
        while ( cursor.shouldRetry() );

        if ( isInternal )
        {
            TreeNode.goTo( cursor, "child", leftmostSibling );
        }
        return isInternal;
    }

    private static void assertAllIdsOccupied( long highId, BitSet seenIds )
    {
        long expectedNumberOfPages = highId - IdSpace.MIN_TREE_NODE_ID;
        if ( seenIds.cardinality() != expectedNumberOfPages )
        {
            StringBuilder builder = new StringBuilder( "[" );
            int index = (int) IdSpace.MIN_TREE_NODE_ID;
            int count = 0;
            while ( index >= 0 && index < highId )
            {
                index = seenIds.nextClearBit( index );
                if ( index != -1 )
                {
                    if ( count++ > 0 )
                    {
                        builder.append( "," );
                    }
                    builder.append( index );
                    index++;
                }
            }
            builder.append( "]" );
            throw new RuntimeException( "There are " + count + " unused pages in the store:" + builder );
        }
    }

    private void traverseAndAddRightSiblings( PageCursor cursor, BitSet seenIds, long lastId ) throws IOException
    {
        long rightSibling;
        do
        {
            do
            {
                rightSibling = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
            }
            while ( cursor.shouldRetry() );

            if ( TreeNode.isNode( rightSibling ) )
            {
                TreeNode.goTo( cursor, "right sibling", rightSibling );
                addToSeenList( seenIds, pointer( rightSibling ), lastId );
            }
        }
        while ( TreeNode.isNode( rightSibling ) );
    }

    private static void addToSeenList( BitSet target, long id, long lastId )
    {
        int index = toIntExact( id );
        if ( target.get( index ) )
        {
            throw new IllegalStateException( id + " already seen" );
        }
        if ( id > lastId )
        {
            throw new IllegalStateException( "Unexpectedly high id " + id + " seen when last id is " + lastId );
        }
        target.set( index );
    }

    private void checkSubtree( PageCursor cursor, KeyRange<KEY> range, long parentNode, long pointerGeneration, GBPTreePointerType parentPointerType, int level,
            GBPTreeConsistencyCheckVisitor<KEY> visitor )
            throws IOException
    {
        byte nodeType;
        byte treeNodeType;
        int keyCount;
        long successor;
        long successorGeneration;

        long leftSiblingPointer;
        long rightSiblingPointer;
        long leftSiblingPointerGeneration;
        long rightSiblingPointerGeneration;
        long currentNodeGeneration;

        do
        {
            // check header pointers
            assertNoCrashOrBrokenPointerInGSPP(
                    cursor, stableGeneration, unstableGeneration, "LeftSibling", TreeNode.BYTE_POS_LEFTSIBLING );
            assertNoCrashOrBrokenPointerInGSPP(
                    cursor, stableGeneration, unstableGeneration, "RightSibling", TreeNode.BYTE_POS_RIGHTSIBLING );
            assertNoCrashOrBrokenPointerInGSPP(
                    cursor, stableGeneration, unstableGeneration, "Successor", TreeNode.BYTE_POS_SUCCESSOR );

            // for assertSiblings
            leftSiblingPointer = TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration, generationTarget );
            leftSiblingPointerGeneration = generationTarget.generation;
            rightSiblingPointer = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration, generationTarget );
            rightSiblingPointerGeneration = generationTarget.generation;
            leftSiblingPointer = pointer( leftSiblingPointer );
            rightSiblingPointer = pointer( rightSiblingPointer );
            currentNodeGeneration = TreeNode.generation( cursor );

            successor = TreeNode.successor( cursor, stableGeneration, unstableGeneration, generationTarget );
            successorGeneration = generationTarget.generation;

            keyCount = TreeNode.keyCount( cursor );
            nodeType = TreeNode.nodeType( cursor );
            treeNodeType = TreeNode.treeNodeType( cursor );
            if ( !node.reasonableKeyCount( keyCount ) )
            {
                // todo change this to report inconsistency instead
                cursor.setCursorException( "Unexpected keyCount:" + keyCount );
            }
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE )
        {
             visitor.notATreeNode( cursor.getCurrentPageId() );
        }

        boolean isLeaf = treeNodeType == TreeNode.LEAF_FLAG;
        boolean isInternal = treeNodeType == TreeNode.INTERNAL_FLAG;
        if ( !isInternal && !isLeaf )
        {
            visitor.unknownTreeNodeType( cursor.getCurrentPageId(), treeNodeType );
        }

        assertKeyOrder( cursor, range, keyCount, isLeaf ? LEAF : INTERNAL, visitor );

        do
        {
            node.checkMetaConsistency( cursor, keyCount, isLeaf ? LEAF : INTERNAL, visitor );
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        assertPointerGenerationMatchesGeneration( parentPointerType, parentNode, cursor.getCurrentPageId(), pointerGeneration,
                currentNodeGeneration, visitor );
        assertSiblings( cursor, currentNodeGeneration, leftSiblingPointer, leftSiblingPointerGeneration, rightSiblingPointer,
                rightSiblingPointerGeneration, level, visitor );
        checkSuccessorPointerGeneration( cursor, successor, successorGeneration, visitor );

        if ( isInternal )
        {
            assertSubtrees( cursor, range, keyCount, level, visitor );
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

    private void checkSuccessorPointerGeneration( PageCursor cursor, long successor, long successorGeneration,
            GBPTreeConsistencyCheckVisitor<KEY> visitor )
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
            GBPTreeConsistencyCheckVisitor<KEY> visitor ) throws IOException
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
            do
            {
                child = childAt( cursor, pos, generationTarget );
                childGeneration = generationTarget.generation;
                node.keyAt( cursor, readKey, pos, INTERNAL );
            }
            while ( cursor.shouldRetry() );
            checkAfterShouldRetry( cursor );

            childRange = range.restrictRight( readKey );
            if ( pos > 0 )
            {
                childRange = childRange.narrowLeft( prev );
            }

            TreeNode.goTo( cursor, "child at pos " + pos, child );
            checkSubtree( cursor, childRange, pageId, childGeneration, GBPTreePointerType.child( pos ), level + 1, visitor );

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
        do
        {
            child = childAt( cursor, pos, generationTarget );
            childGeneration = generationTarget.generation;
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        TreeNode.goTo( cursor, "child at pos " + pos, child );
        childRange = range.restrictLeft( prev );
        checkSubtree( cursor, childRange, pageId, childGeneration, GBPTreePointerType.child( pos ), level + 1, visitor );
        TreeNode.goTo( cursor, "parent", pageId );
    }

    private static void checkAfterShouldRetry( PageCursor cursor ) throws CursorException
    {
        checkOutOfBounds( cursor );
        cursor.checkAndClearCursorException();
    }

    private long childAt( PageCursor cursor, int pos, GenerationTarget childGeneration )
    {
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, "Child", node.childOffset( pos ) );
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

    static void assertNoCrashOrBrokenPointerInGSPP( PageCursor cursor, long stableGeneration, long unstableGeneration,
            String pointerFieldName, int offset )
    {
        cursor.setOffset( offset );
        long currentNodeId = cursor.getCurrentPageId();
        // A
        long generationA = GenerationSafePointer.readGeneration( cursor );
        long readPointerA = GenerationSafePointer.readPointer( cursor );
        long pointerA = GenerationSafePointerPair.pointer( readPointerA );
        short checksumA = GenerationSafePointer.readChecksum( cursor );
        boolean correctChecksumA = GenerationSafePointer.checksumOf( generationA, readPointerA ) == checksumA;
        byte stateA = GenerationSafePointerPair.pointerState(
                stableGeneration, unstableGeneration, generationA, readPointerA, correctChecksumA );
        boolean okA = stateA != GenerationSafePointerPair.BROKEN && stateA != GenerationSafePointerPair.CRASH;

        // B
        long generationB = GenerationSafePointer.readGeneration( cursor );
        long readPointerB = GenerationSafePointer.readPointer( cursor );
        long pointerB = GenerationSafePointerPair.pointer( readPointerA );
        short checksumB = GenerationSafePointer.readChecksum( cursor );
        boolean correctChecksumB = GenerationSafePointer.checksumOf( generationB, readPointerB ) == checksumB;
        byte stateB = GenerationSafePointerPair.pointerState(
                stableGeneration, unstableGeneration, generationB, readPointerB, correctChecksumB );
        boolean okB = stateB != GenerationSafePointerPair.BROKEN && stateB != GenerationSafePointerPair.CRASH;

        if ( !(okA && okB) )
        {
            boolean isInternal = TreeNode.isInternal( cursor );
            String type = isInternal ? "internal" : "leaf";
            cursor.setCursorException( format(
                    "GSPP state found that was not ok in %s field in %s node with id %d%n  slotA[%s]%n  slotB[%s]",
                    pointerFieldName, type, currentNodeId,
                    stateToString( generationA, readPointerA, pointerA, stateA ),
                    stateToString( generationB, readPointerB, pointerB, stateB ) ) );
        }
    }

    private static String stateToString( long generation, long readPointer, long pointer, byte stateA )
    {
        return format( "generation=%d, readPointer=%d, pointer=%d, state=%s",
                generation, readPointer, pointer, GenerationSafePointerPair.pointerStateName( stateA ) );
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
}
