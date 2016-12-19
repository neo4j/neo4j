/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.gbptree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;

import static org.neo4j.index.gbptree.GenSafePointerPair.pointer;

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
    private final List<RightmostInChain<KEY>> rightmostPerLevel = new ArrayList<>();
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

    public boolean check( PageCursor cursor, long expectedGen ) throws IOException
    {
        assertOnTreeNode( cursor );
        KeyRange<KEY> openRange = new KeyRange<>( comparator, null, null, layout, null );
        boolean result = checkSubtree( cursor, openRange, expectedGen, 0 );

        // Assert that rightmost node on each level has empty right sibling.
        rightmostPerLevel.forEach( RightmostInChain::assertLast );
        return result;
    }

    /**
     * Checks so that all pages between {@link IdSpace#MIN_TREE_NODE_ID} and highest allocated id
     * are either in use in the tree, on the free-list or free-list nodes.
     *
     * @param cursor {@link PageCursor} to use for reading.
     * @return {@code true} if all pages are taken, otherwise {@code false}. Also is compatible with java
     * assert calls.
     * @throws IOException on {@link PageCursor} error.
     */
    public boolean checkSpace( PageCursor cursor, long lastId, PrimitiveLongIterator freelistIds ) throws IOException
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
            node.goTo( cursor, "back", leftmostSibling );
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
            isInternal = node.isInternal( cursor );
            if ( isInternal )
            {
                leftmostSibling = node.childAt( cursor, 0, stableGeneration, unstableGeneration );
            }
        }
        while ( cursor.shouldRetry() );

        if ( isInternal )
        {
            node.goTo( cursor, "child", leftmostSibling );
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
                rightSibling = node.rightSibling( cursor, stableGeneration, unstableGeneration );
            }
            while ( cursor.shouldRetry() );

            if ( TreeNode.isNode( rightSibling ) )
            {
                node.goTo( cursor, "right sibling", rightSibling );
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

    private void assertOnTreeNode( PageCursor cursor )
    {
        if ( TreeNode.nodeType( cursor ) != TreeNode.NODE_TYPE_TREE_NODE )
        {
            throw new IllegalArgumentException( "Cursor is not pinned to a tree node page. pageId:" +
                    cursor.getCurrentPageId() );
        }
        if ( !node.isInternal( cursor ) && !node.isLeaf( cursor ) )
        {
            throw new IllegalArgumentException( "Cursor is not pinned to a page containing a tree node. pageId:" +
                    cursor.getCurrentPageId() );
        }
    }

    private boolean checkSubtree( PageCursor cursor, KeyRange<KEY> range, long expectedGen, int level )
            throws IOException
    {
        // check header pointers
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, "LeftSibling", TreeNode.BYTE_POS_LEFTSIBLING, node );
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, "RightSibling", TreeNode.BYTE_POS_RIGHTSIBLING, node );
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, "NewGen", TreeNode.BYTE_POS_NEWGEN, node );

        long pageId = assertSiblings( cursor, level );

        checkNewGenPointerGen( cursor );

        assertPointerGenMatchesGen( cursor, expectedGen );

        if ( node.isInternal( cursor ) )
        {
            assertKeyOrderAndSubtrees( cursor, pageId, range, level );
        }
        else if ( node.isLeaf( cursor ) )
        {
            assertKeyOrder( cursor, range );
        }
        else
        {
            throw new TreeInconsistencyException( "Page:" + cursor.getCurrentPageId() + " at level:" + level +
                    " isn't a tree node, parent expected range " + range );
        }
        return true;
    }

    private void assertPointerGenMatchesGen( PageCursor cursor, long expectedGen )
    {
        long nodeGen = node.gen( cursor );
        assert nodeGen <= expectedGen : "Expected node:" + cursor.getCurrentPageId() + " gen:" + nodeGen +
                " to be ≤ pointer gen:" + expectedGen;
    }

    private void checkNewGenPointerGen( PageCursor cursor ) throws IOException
    {
        long newGen = node.newGen( cursor, stableGeneration, unstableGeneration );
        if ( TreeNode.isNode( newGen ) )
        {
            System.err.println( "WARNING: we ended up on an old generation " + cursor.getCurrentPageId() +
                    " which had newGen:" + pointer( newGen ) );
            long newGenGen = node.pointerGen( cursor, newGen );
            long origin = cursor.getCurrentPageId();
            node.goTo( cursor, "newGen", newGen );
            try
            {
                assertPointerGenMatchesGen( cursor, newGenGen );
            }
            finally
            {
                node.goTo( cursor, "back", origin );
            }
        }
    }

    // Assumption: We traverse the tree from left to right on every level
    private long assertSiblings( PageCursor cursor, int level )
    {
        // If this is the first time on this level, we will add a new entry
        for ( int i = rightmostPerLevel.size(); i <= level; i++ )
        {
            RightmostInChain<KEY> first = new RightmostInChain<>( node, stableGeneration, unstableGeneration );
            rightmostPerLevel.add( i, first );
        }
        RightmostInChain<KEY> rightmost = rightmostPerLevel.get( level );

        return rightmost.assertNext( cursor );
    }

    private void assertKeyOrderAndSubtrees( PageCursor cursor, long pageId, KeyRange<KEY> range, int level )
            throws IOException
    {
        int keyCount = node.keyCount( cursor );
        KEY prev = layout.newKey();
        KeyRange<KEY> childRange;

        int pos = 0;
        while ( pos < keyCount )
        {
            node.keyAt( cursor, readKey, pos );
            assert range.inRange( readKey ) :
                readKey + " (pos " + pos + "/" + (keyCount-1) + ")" + " isn't in range " + range;
            if ( pos > 0 )
            {
                assert comparator.compare( prev, readKey ) < 0; // Assume unique keys
            }

            long child = childAt( cursor, pos );
            long childGen = node.pointerGen( cursor, child );
            node.goTo( cursor, "child at pos " + pos, child );
            if ( pos == 0 )
            {
                childRange = range.restrictRight( readKey );
            }
            else
            {
                childRange = range.restrictLeft( prev ).restrictRight( readKey );
            }
            checkSubtree( cursor, childRange, childGen, level + 1 );
            node.goTo( cursor, "parent", pageId );

            layout.copyKey( readKey, prev );
            pos++;
        }

        // Check last child
        long child = childAt( cursor, pos );
        long childGen = node.pointerGen( cursor, child );
        node.goTo( cursor, "child at pos " + pos, child );
        childRange = range.restrictLeft( prev );
        checkSubtree( cursor, childRange, childGen, level + 1 );
        node.goTo( cursor, "parent", pageId );
    }

    private long childAt( PageCursor cursor, int pos )
    {
        assertNoCrashOrBrokenPointerInGSPP(
                cursor, stableGeneration, unstableGeneration, "Child", node.childOffset( pos ), node );
        return node.childAt( cursor, pos, stableGeneration, unstableGeneration );
    }

    private void assertKeyOrder( PageCursor cursor, KeyRange<KEY> range )
    {
        int keyCount = node.keyCount( cursor );
        KEY prev = layout.newKey();
        boolean first = true;
        for ( int pos = 0; pos < keyCount; pos++ )
        {
            node.keyAt( cursor, readKey, pos );
            assert range.inRange( readKey ) : "Expected " + readKey + " to be in range " + range;
            if ( !first )
            {
                assert comparator.compare( prev, readKey ) < 0; // Assume unique keys
            }
            else
            {
                first = false;
            }
            layout.copyKey( readKey, prev );
        }
    }

    static void assertNoCrashOrBrokenPointerInGSPP( PageCursor cursor, long stableGeneration, long unstableGeneration,
            String pointerFieldName, int offset, TreeNode<?,?> treeNode )
    {
        cursor.setOffset( offset );
        long currentNodeId = cursor.getCurrentPageId();
        // A
        long generationA = GenSafePointer.readGeneration( cursor );
        long pointerA = GenSafePointer.readPointer( cursor );
        short checksumA = GenSafePointer.readChecksum( cursor );
        boolean correctChecksumA = GenSafePointer.checksumOf( generationA, pointerA ) == checksumA;
        byte stateA = GenSafePointerPair.pointerState(
                stableGeneration, unstableGeneration, generationA, pointerA, correctChecksumA );
        boolean okA = stateA != GenSafePointerPair.BROKEN && stateA != GenSafePointerPair.CRASH;

        // B
        long generationB = GenSafePointer.readGeneration( cursor );
        long pointerB = GenSafePointer.readPointer( cursor );
        short checksumB = GenSafePointer.readChecksum( cursor );
        boolean correctChecksumB = GenSafePointer.checksumOf( generationB, pointerB ) == checksumB;
        byte stateB = GenSafePointerPair.pointerState(
                stableGeneration, unstableGeneration, generationB, pointerB, correctChecksumB );
        boolean okB = stateB != GenSafePointerPair.BROKEN && stateB != GenSafePointerPair.CRASH;

        if ( !(okA && okB) )
        {
            boolean isInternal = treeNode.isInternal( cursor );
            String type = isInternal ? "internal" : "leaf";
            throw new TreeInconsistencyException(
                    "GSPP state found that was not ok in %s field in %s node with id %d%n  slotA[%s]%n  slotB[%s]",
                    pointerFieldName, type, currentNodeId,
                    stateToString( generationA, pointerA, stateA ),
                    stateToString( generationB, pointerB, stateB ) );
        }
    }

    private static String stateToString( long generationA, long pointerA, byte stateA )
    {
        return format( "generation=%d, pointer=%d, state=%s",
                generationA, pointerA, GenSafePointerPair.pointerStateName( stateA ) );
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
            if ( fromInclusive == null || comparator.compare( fromInclusive, left ) < 0 )
            {
                return new KeyRange<>( comparator, left, toExclusive, layout, this );
            }
            return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
        }

        KeyRange<KEY> restrictRight( KEY right )
        {
            if ( toExclusive == null || comparator.compare( toExclusive, right ) > 0 )
            {
                return new KeyRange<>( comparator, fromInclusive, right, layout, this );
            }
            return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
        }

        @Override
        public String toString()
        {
            return fromInclusive + " ≤ key < " + toExclusive + (superRange != null ? format( "%n%s", superRange ) : "");
        }
    }
}
