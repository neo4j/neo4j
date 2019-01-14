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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
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
class ConsistencyChecker<KEY>
{
    private final TreeNode<KEY,?> node;
    private final KEY readKey;
    private final Comparator<KEY> comparator;
    private final Layout<KEY,?> layout;
    private final List<RightmostInChain> rightmostPerLevel = new ArrayList<>();
    private final long stableGeneration;
    private final long unstableGeneration;

    ConsistencyChecker( TreeNode<KEY,?> node, Layout<KEY,?> layout, long stableGeneration, long unstableGeneration )
    {
        this.node = node;
        this.readKey = layout.newKey();
        this.comparator = node.keyComparator();
        this.layout = layout;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
    }

    public boolean check( PageCursor cursor, long expectedGeneration ) throws IOException
    {
        assertOnTreeNode( cursor );
        KeyRange<KEY> openRange = new KeyRange<>( comparator, null, null, layout, null );
        boolean result = checkSubtree( cursor, openRange, expectedGeneration, 0 );

        // Assert that rightmost node on each level has empty right sibling.
        rightmostPerLevel.forEach( RightmostInChain::assertLast );
        return result;
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
    boolean checkSpace( PageCursor cursor, long lastId, PrimitiveLongIterator freelistIds ) throws IOException
    {
        assertOnTreeNode( cursor );

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

    static void assertOnTreeNode( PageCursor cursor ) throws IOException
    {
        byte nodeType;
        boolean isInternal;
        boolean isLeaf;
        do
        {
            nodeType = TreeNode.nodeType( cursor );
            isInternal = TreeNode.isInternal( cursor );
            isLeaf = TreeNode.isLeaf( cursor );
        }
        while ( cursor.shouldRetry() );

        if ( nodeType != TreeNode.NODE_TYPE_TREE_NODE )
        {
            throw new IllegalArgumentException( "Cursor is not pinned to a tree node page. pageId:" +
                    cursor.getCurrentPageId() );
        }
        if ( !isInternal && !isLeaf )
        {
            throw new IllegalArgumentException( "Cursor is not pinned to a page containing a tree node. pageId:" +
                    cursor.getCurrentPageId() );
        }
    }

    private boolean checkSubtree( PageCursor cursor, KeyRange<KEY> range, long expectedGeneration, int level )
            throws IOException
    {
        boolean isInternal = false;
        boolean isLeaf = false;
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
            leftSiblingPointer = TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration );
            rightSiblingPointer = TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
            leftSiblingPointerGeneration = node.pointerGeneration( cursor, leftSiblingPointer );
            rightSiblingPointerGeneration = node.pointerGeneration( cursor, rightSiblingPointer );
            leftSiblingPointer = pointer( leftSiblingPointer );
            rightSiblingPointer = pointer( rightSiblingPointer );
            currentNodeGeneration = TreeNode.generation( cursor );

            successor = TreeNode.successor( cursor, stableGeneration, unstableGeneration );
            successorGeneration = node.pointerGeneration( cursor, successor );

            keyCount = TreeNode.keyCount( cursor );
            if ( !node.reasonableKeyCount( keyCount ) )
            {
                cursor.setCursorException( "Unexpected keyCount:" + keyCount );
                continue;
            }
            isInternal = TreeNode.isInternal( cursor );
            isLeaf = TreeNode.isLeaf( cursor );
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        if ( !isInternal && !isLeaf )
        {
            throw new TreeInconsistencyException( "Page:" + cursor.getCurrentPageId() + " at level:" + level +
                    " isn't a tree node, parent expected range " + range );
        }

        do
        {
            assertKeyOrder( cursor, range, keyCount, isLeaf ? LEAF : INTERNAL );
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        assertPointerGenerationMatchesGeneration( cursor, currentNodeGeneration, expectedGeneration );
        assertSiblings( cursor, currentNodeGeneration, leftSiblingPointer, leftSiblingPointerGeneration, rightSiblingPointer,
                rightSiblingPointerGeneration, level );
        checkSuccessorPointerGeneration( cursor, successor, successorGeneration );

        if ( isInternal )
        {
            assertSubtrees( cursor, range, keyCount, level );
        }
        return true;
    }

    private static void assertPointerGenerationMatchesGeneration( PageCursor cursor, long nodeGeneration,
            long expectedGeneration )
    {
        if ( nodeGeneration > expectedGeneration )
        {
            throw new TreeInconsistencyException( "Expected node:%d generation:%d to be ≤ pointer generation:%d", cursor.getCurrentPageId(), nodeGeneration,
                    expectedGeneration );
        }
    }

    private void checkSuccessorPointerGeneration( PageCursor cursor, long successor, long successorGeneration )
            throws IOException
    {
        if ( TreeNode.isNode( successor ) )
        {
            cursor.setCursorException( "WARNING: we ended up on an old generation " + cursor.getCurrentPageId() +
                    " which had successor:" + pointer( successor ) );
            long origin = cursor.getCurrentPageId();
            TreeNode.goTo( cursor, "successor", successor );
            try
            {
                long nodeGeneration;
                do
                {
                    nodeGeneration = TreeNode.generation( cursor );
                }
                while ( cursor.shouldRetry() );
                checkAfterShouldRetry( cursor );

                assertPointerGenerationMatchesGeneration( cursor, nodeGeneration, successorGeneration );
            }
            finally
            {
                TreeNode.goTo( cursor, "back", origin );
            }
        }
    }

    // Assumption: We traverse the tree from left to right on every level
    private void assertSiblings( PageCursor cursor, long currentNodeGeneration, long leftSiblingPointer,
            long leftSiblingPointerGeneration, long rightSiblingPointer, long rightSiblingPointerGeneration, int level )
    {
        // If this is the first time on this level, we will add a new entry
        for ( int i = rightmostPerLevel.size(); i <= level; i++ )
        {
            rightmostPerLevel.add( i, new RightmostInChain() );
        }
        RightmostInChain rightmost = rightmostPerLevel.get( level );

        rightmost.assertNext( cursor, currentNodeGeneration, leftSiblingPointer, leftSiblingPointerGeneration, rightSiblingPointer,
                rightSiblingPointerGeneration );
    }

    private void assertSubtrees( PageCursor cursor, KeyRange<KEY> range, int keyCount, int level )
            throws IOException
    {
        long pageId = cursor.getCurrentPageId();
        KEY prev = null;
        KeyRange<KEY> childRange;

        // Check children, all except the last one
        int pos = 0;
        while ( pos < keyCount )
        {
            long child;
            long childGeneration;
            do
            {
                child = childAt( cursor, pos );
                childGeneration = node.pointerGeneration( cursor, child );
                node.keyAt( cursor, readKey, pos, INTERNAL );
            }
            while ( cursor.shouldRetry() );
            checkAfterShouldRetry( cursor );

            childRange = range.restrictRight( readKey );
            if ( pos > 0 )
            {
                childRange = range.restrictLeft( prev );
            }

            TreeNode.goTo( cursor, "child at pos " + pos, child );
            checkSubtree( cursor, childRange, childGeneration, level + 1 );

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
            child = childAt( cursor, pos );
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        do
        {
            childGeneration = node.pointerGeneration( cursor, child );
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        TreeNode.goTo( cursor, "child at pos " + pos, child );
        childRange = range.restrictLeft( prev );
        checkSubtree( cursor, childRange, childGeneration, level + 1 );
        TreeNode.goTo( cursor, "parent", pageId );
    }

    private static void checkAfterShouldRetry( PageCursor cursor ) throws CursorException
    {
        checkOutOfBounds( cursor );
        cursor.checkAndClearCursorException();
    }

    private long childAt( PageCursor cursor, int pos )
    {
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, "Child", node.childOffset( pos ) );
        return node.childAt( cursor, pos, stableGeneration, unstableGeneration );
    }

    private void assertKeyOrder( PageCursor cursor, KeyRange<KEY> range, int keyCount, TreeNode.Type type )
    {
        KEY prev = layout.newKey();
        boolean first = true;
        for ( int pos = 0; pos < keyCount; pos++ )
        {
            node.keyAt( cursor, readKey, pos, type );
            if ( !range.inRange( readKey ) )
            {
                cursor.setCursorException(
                        format( "Expected range for this node is %n%s%n but found %s in position %d, with keyCount %d on page %d",
                        range, readKey, pos, keyCount, cursor.getCurrentPageId() ) );
            }
            if ( !first )
            {
                if ( comparator.compare( prev, readKey ) >= 0 )
                {
                    cursor.setCursorException( "Non-unique key " + readKey );
                }
            }
            else
            {
                first = false;
            }
            layout.copyKey( readKey, prev );
        }
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

    private static class KeyRange<KEY>
    {
        private final Comparator<KEY> comparator;
        private final KEY fromInclusive;
        private final KEY toExclusive;
        private final Layout<KEY,?> layout;
        private final KeyRange<KEY> superRange;

        private KeyRange( Comparator<KEY> comparator, KEY fromInclusive, KEY toExclusive, Layout<KEY,?> layout,
                KeyRange<KEY> superRange )
        {
            this.comparator = comparator;
            this.superRange = superRange;
            this.fromInclusive = fromInclusive == null ? null : layout.copyKey( fromInclusive, layout.newKey() );
            this.toExclusive = toExclusive == null ? null : layout.copyKey( toExclusive, layout.newKey() );
            this.layout = layout;
        }

        boolean inRange( KEY key )
        {
            if ( fromInclusive != null )
            {
                if ( toExclusive != null )
                {
                    return comparator.compare( key, fromInclusive ) >= 0 && comparator.compare( key, toExclusive ) < 0;
                }
                return comparator.compare( key, fromInclusive ) >= 0;
            }
            return toExclusive == null || comparator.compare( key, toExclusive ) < 0;
        }

        KeyRange<KEY> restrictLeft( KEY left )
        {
            if ( fromInclusive == null )
            {
                return new KeyRange<>( comparator, left, toExclusive, layout, this );
            }
            if ( left == null )
            {
                return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
            }
            if ( comparator.compare( fromInclusive, left ) < 0 )
            {
                return new KeyRange<>( comparator, left, toExclusive, layout, this );
            }
            return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
        }

        KeyRange<KEY> restrictRight( KEY right )
        {
            if ( toExclusive == null )
            {
                return new KeyRange<>( comparator, fromInclusive, right, layout, this );
            }
            if ( right == null )
            {
                return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
            }
            if ( comparator.compare( toExclusive, right ) > 0 )
            {
                return new KeyRange<>( comparator, fromInclusive, right, layout, this );
            }
            return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
        }

        @Override
        public String toString()
        {
            return (superRange != null ? format( "%s%n", superRange ) : "") + fromInclusive + " ≤ key < " + toExclusive;
        }
    }
}
