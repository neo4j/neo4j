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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.index.internal.gbptree.TreeNode.Section;
import org.neo4j.index.internal.gbptree.TreeNode.Type;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;

import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;

/**
 * <ul>
 * Generally: for leafs, both main and delta sections are checked
 * Checks:
 * <li>order of keys in internal nodes
 * <li>keys fit inside range given by parent node
 * <li>sibling pointers match
 * <li>GSPP
 * </ul>
 */
class ConsistencyChecker<KEY>
{
    private final TreeNode<KEY,?> node;
    private final Section<KEY,?> mainSection;
    private final Section<KEY,?> deltaSection;
    private final KEY readKey;
    private final KEY readDeltaKey;
    private final Comparator<KEY> comparator;
    private final Layout<KEY,?> layout;
    private final List<RightmostInChain> rightmostPerLevel = new ArrayList<>();
    private final long stableGeneration;
    private final long unstableGeneration;

    ConsistencyChecker( TreeNode<KEY,?> node, Layout<KEY,?> layout, long stableGeneration, long unstableGeneration )
    {
        this.node = node;
        this.mainSection = node.main();
        this.deltaSection = node.delta();
        this.readKey = layout.newKey();
        this.readDeltaKey = layout.newKey();
        this.comparator = mainSection.keyComparator();
        this.layout = layout;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
    }

    /**
     * Checks consistency of tree from where {@code cursor} is when calling the method, typically the root.
     *
     * @param cursor {@link PageCursor} placed at the root of the tree to check.
     * @param expectedGeneration expected generation of the starting tree node.
     * @return {@code true} if tree is consistent, otherwise throws {@link TreeInconsistencyException}.
     * This is set to return boolean so that it can easily be used in {@code assert} checks.
     * @throws IOException on page reading errors.
     * @throws TreeInconsistencyException on first found inconsistency, if any.
     */
    public boolean check( PageCursor cursor, long expectedGeneration ) throws IOException
    {
        assertOnTreeNode( node, cursor );
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
        assertOnTreeNode( node, cursor );

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
                leftmostSibling = mainSection.childAt( cursor, 0, stableGeneration, unstableGeneration );
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
            throw new TreeInconsistencyException( "There are %d unused pages in the store:%s", count, builder );
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

    static void assertOnTreeNode( TreeNode<?,?> node, PageCursor cursor ) throws IOException
    {
        byte nodeType;
        boolean isInternal;
        boolean isLeaf;
        do
        {
            nodeType = TreeNode.nodeType( cursor );
            isInternal = node.isInternal( cursor );
            isLeaf = node.isLeaf( cursor );
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
        long successorGeneration = 0;

        long leftSiblingPointer;
        long rightSiblingPointer;
        long leftSiblingPointerGeneration = 0;
        long rightSiblingPointerGeneration = 0;
        long currentNodeGeneration;

        do
        {
            // check header pointers
            assertNoCrashOrBrokenPointerInGSPP(
                    node, cursor, stableGeneration, unstableGeneration, "LeftSibling", node.leftSiblingOffset() );
            assertNoCrashOrBrokenPointerInGSPP(
                    node, cursor, stableGeneration, unstableGeneration, "RightSibling", node.rightSiblingOffset() );
            assertNoCrashOrBrokenPointerInGSPP(
                    node, cursor, stableGeneration, unstableGeneration, "Successor", node.successorOffset() );

            // for assertSiblings
            leftSiblingPointer = node.leftSibling( cursor, stableGeneration, unstableGeneration );
            if ( GenerationSafePointerPair.isSuccess( leftSiblingPointer ) )
            {
                leftSiblingPointerGeneration = node.pointerGeneration( cursor, leftSiblingPointer );
            }
            leftSiblingPointer = pointer( leftSiblingPointer );

            rightSiblingPointer = node.rightSibling( cursor, stableGeneration, unstableGeneration );
            if ( GenerationSafePointerPair.isSuccess( rightSiblingPointer ) )
            {
                rightSiblingPointerGeneration = node.pointerGeneration( cursor, rightSiblingPointer );
            }
            rightSiblingPointer = pointer( rightSiblingPointer );
            currentNodeGeneration = node.generation( cursor );

            successor = node.successor( cursor, stableGeneration, unstableGeneration );
            if ( GenerationSafePointerPair.isSuccess( successor ) )
            {
                successorGeneration = node.pointerGeneration( cursor, successor );
            }
            successor = pointer( successor );

            isInternal = node.isInternal( cursor );
            isLeaf = node.isLeaf( cursor );

            keyCount = mainSection.keyCount( cursor );
            if ( keyCount > mainSection.internalMaxKeyCount() && keyCount > mainSection.leafMaxKeyCount() )
            {
                cursor.setCursorException( "Unexpected main keyCount:" + keyCount );
                continue;
            }
            int deltaKeyCount = 0;
            if ( isLeaf )
            {
                deltaKeyCount = deltaSection.keyCount( cursor );
                if ( deltaKeyCount > deltaSection.leafMaxKeyCount() )
                {
                    cursor.setCursorException( "Unexpected delta keyCount:" + deltaKeyCount );
                    continue;
                }
            }
            assertKeyOrder( cursor, range, keyCount, deltaKeyCount );
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        if ( !isInternal && !isLeaf )
        {
            throw new TreeInconsistencyException( "Page:" + cursor.getCurrentPageId() + " at level:" + level +
                    " isn't a tree node, parent expected range " + range );
        }

        assertPointerGenerationMatchesGeneration( cursor, currentNodeGeneration, expectedGeneration );
        assertSiblings( cursor, currentNodeGeneration, leftSiblingPointer, leftSiblingPointerGeneration, rightSiblingPointer,
                rightSiblingPointerGeneration, level );
        checkCursorException( cursor );
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
        assert nodeGeneration <= expectedGeneration : "Expected node:" + cursor.getCurrentPageId() + " generation:" + nodeGeneration +
                " to be ≤ pointer generation:" + expectedGeneration;
    }

    private void checkSuccessorPointerGeneration( PageCursor cursor, long successor, long successorPointerGeneration )
            throws IOException
    {
        if ( TreeNode.isNode( successor ) )
        {
            node.goTo( cursor, "successor", successor );
            long successorGeneration;
            do
            {
                successorGeneration = node.generation( cursor );
            }
            while ( cursor.shouldRetry() );

            throw new TreeInconsistencyException( "Ended up on tree node:%d which has successor:%d with generation:%d " +
                    "(pointer said generation:%d)",
                    cursor.getCurrentPageId(), pointer( successor ), successorGeneration, successorPointerGeneration );
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
        KEY prev = layout.newKey();
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
                mainSection.keyAt( cursor, readKey, pos );
            }
            while ( cursor.shouldRetry() );
            checkAfterShouldRetry( cursor );

            childRange = range.restrictRight( readKey );
            if ( pos > 0 )
            {
                childRange = range.restrictLeft( prev );
            }

            node.goTo( cursor, "child at pos " + pos, child );
            checkSubtree( cursor, childRange, childGeneration, level + 1 );

            node.goTo( cursor, "parent", pageId );

            layout.copyKey( readKey, prev );
            pos++;
        }

        // Check last child
        long child;
        long childGeneration;
        do
        {
            child = childAt( cursor, pos );
            childGeneration = node.pointerGeneration( cursor, child );
        }
        while ( cursor.shouldRetry() );
        checkAfterShouldRetry( cursor );

        node.goTo( cursor, "child at pos " + pos, child );
        childRange = range.restrictLeft( prev );
        checkSubtree( cursor, childRange, childGeneration, level + 1 );
        node.goTo( cursor, "parent", pageId );
    }

    private static void checkAfterShouldRetry( PageCursor cursor )
    {
        checkOutOfBounds( cursor );
        checkCursorException( cursor );
    }

    private static void checkCursorException( PageCursor cursor )
    {
        try
        {
            cursor.checkAndClearCursorException();
        }
        catch ( CursorException e )
        {
            throw new TreeInconsistencyException( e.getMessage() );
        }
    }

    private long childAt( PageCursor cursor, int pos )
    {
        assertNoCrashOrBrokenPointerInGSPP(
                node, cursor, stableGeneration, unstableGeneration, "Child", node.childOffset( pos ) );
        return mainSection.childAt( cursor, pos, stableGeneration, unstableGeneration );
    }

    private void assertKeyOrder( PageCursor cursor, KeyRange<KEY> range, int mainKeyCount, int deltaKeyCount )
    {
        KEY prev = layout.newKey();
        boolean first = true;
        if ( mainKeyCount > 0 )
        {
            mainSection.keyAt( cursor, readKey, 0 );
        }
        if ( deltaKeyCount > 0 )
        {
            deltaSection.keyAt( cursor, readDeltaKey, 0 );
        }
        Type sectionType = Type.MAIN;
        for ( int mainPos = 0, deltaPos = 0; mainPos < mainKeyCount || deltaPos < deltaKeyCount; )
        {
            Section<?,?> section;
            KEY key;
            int pos;
            int keyCount;
            if ( mainPos < mainKeyCount && deltaPos < deltaKeyCount )
            {
                section = layout.compare( readKey, readDeltaKey ) < 0 ? mainSection : deltaSection;
            }
            else
            {
                section = mainPos < mainKeyCount ? mainSection : deltaSection;
            }

            if ( section == mainSection )
            {
                key = readKey;
                pos = mainPos++;
                keyCount = mainKeyCount;
            }
            else
            {
                key = readDeltaKey;
                pos = deltaPos++;
                keyCount = deltaKeyCount;
            }
            sectionType = section.type();

            if ( !range.inRange( key ) )
            {
                cursor.setCursorException( "Expected range for this node is " + range + " but found " +
                        key + " in position " + pos + ", with key count " + keyCount + " in section " + section.type() );
            }
            if ( !first )
            {
                if ( comparator.compare( prev, key ) >= 0 )
                {
                    cursor.setCursorException( "Non-unique key " + key + " in position " + pos + " in section " + section.type() );
                }
            }
            else
            {
                first = false;
            }
            layout.copyKey( key, prev );

            // read next in the section which's pos was incremented
            if ( section == mainSection && mainPos < mainKeyCount )
            {
                mainSection.keyAt( cursor, readKey, mainPos );
            }
            else if ( section == deltaSection && deltaPos < deltaKeyCount )
            {
                deltaSection.keyAt( cursor, readDeltaKey, deltaPos );
            }
        }

        if ( sectionType != Type.MAIN )
        {
            throw new TreeInconsistencyException( "Highest key %s not in main section", prev );
        }
    }

    static void assertNoCrashOrBrokenPointerInGSPP( TreeNode<?,?> node,  PageCursor cursor,
            long stableGeneration, long unstableGeneration, String pointerFieldName, int offset )
    {
        cursor.setOffset( offset );
        long currentNodeId = cursor.getCurrentPageId();
        // A
        long generationA = GenerationSafePointer.readGeneration( cursor );
        long pointerA = GenerationSafePointer.readPointer( cursor );
        short checksumA = GenerationSafePointer.readChecksum( cursor );
        boolean correctChecksumA = GenerationSafePointer.checksumOf( generationA, pointerA ) == checksumA;
        byte stateA = GenerationSafePointerPair.pointerState(
                stableGeneration, unstableGeneration, generationA, pointerA, correctChecksumA );
        boolean okA = stateA != GenerationSafePointerPair.BROKEN && stateA != GenerationSafePointerPair.CRASH;

        // B
        long generationB = GenerationSafePointer.readGeneration( cursor );
        long pointerB = GenerationSafePointer.readPointer( cursor );
        short checksumB = GenerationSafePointer.readChecksum( cursor );
        boolean correctChecksumB = GenerationSafePointer.checksumOf( generationB, pointerB ) == checksumB;
        byte stateB = GenerationSafePointerPair.pointerState(
                stableGeneration, unstableGeneration, generationB, pointerB, correctChecksumB );
        boolean okB = stateB != GenerationSafePointerPair.BROKEN && stateB != GenerationSafePointerPair.CRASH;

        if ( !(okA && okB) )
        {
            boolean isInternal = node.isInternal( cursor );
            String type = isInternal ? "internal" : "leaf";
            cursor.setCursorException( format(
                    "GSPP state found that was not ok in %s field in %s node with id %d%n  slotA[%s]%n  slotB[%s]",
                    pointerFieldName, type, currentNodeId,
                    stateToString( generationA, pointerA, stateA ),
                    stateToString( generationB, pointerB, stateB ) ) );
        }
    }

    private static String stateToString( long generationA, long pointerA, byte stateA )
    {
        return format( "generation=%d, pointer=%d, state=%s",
                generationA, pointerA, GenerationSafePointerPair.pointerStateName( stateA ) );
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
            return (superRange != null ? format( "%s%n", superRange ) : "") + fromInclusive + " ≤ key < " + toExclusive;
        }
    }
}
