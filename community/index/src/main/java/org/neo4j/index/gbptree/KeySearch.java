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

import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Methods for (binary-)searching keys in a tree node.
 */
class KeySearch
{
    private static final int POSITION_MASK = 0x7FFFFFFF;
    private static final int HIT_MASK = 0x80000000;

    /**
     * Search for left most pos such that keyAtPos obeys key <= keyAtPos.
     * Return pos (not offset) of keyAtPos, or key count if no such key exist.
     * <p>
     * On insert, key should be inserted at pos.
     * On seek in internal, child at pos should be followed from internal node.
     * On seek in leaf, value at pos is correct if keyAtPos is equal to key.
     * <p>
     * Implemented as binary search.
     * <p>
     * Leaves cursor on same page as when called. No guarantees on offset.
     *
     * @param cursor {@link PageCursor} pinned to page with node (internal or leaf does not matter)
     * @param bTreeNode {@link TreeNode} that knows how to operate on KEY and VALUE
     * @param key KEY to search for
     * @param readKey KEY to use as temporary storage during calculation.
     * @param keyCount number of keys in node when starting search
     * @return search result where least significant 31 bits are first position i for which
     * bTreeNode.keyComparator().compare( key, bTreeNode.keyAt( i ) <= 0, or keyCount if no such key exists.
     * highest bit (sign bit) says whether or not the exact key was found in the node, if so set to 1, otherwise 0.
     * To extract position from the returned search result, then use {@link #positionOf(int)}.
     * To extract whether or not the exact key was found, then use {@link #isHit(int)}.
     */
    static <KEY,VALUE> int search( PageCursor cursor, TreeNode<KEY,VALUE> bTreeNode, KEY key,
            KEY readKey, int keyCount )
    {
        if ( keyCount == 0 )
        {
            return searchResult( 0, false );
        }

        int lower = 0;
        int higher = keyCount-1;
        int pos;
        boolean hit = false;

        // Compare key with lower and higher and sort out special cases
        Comparator<KEY> comparator = bTreeNode.keyComparator();
        int comparison;

        // key greater than greatest key in node
        if ( comparator.compare( key, bTreeNode.keyAt( cursor, readKey, higher ) ) > 0 )
        {
            pos = keyCount;
        }
        // key smaller than or equal to smallest key in node
        else if ( (comparison = comparator.compare( key, bTreeNode.keyAt( cursor, readKey, lower ) )) <= 0 )
        {
            if ( comparison == 0 )
            {
                hit = true;
            }
            pos = 0;
        }
        else
        {
            // Start binary search
            // If key <= keyAtPos -> move higher to pos
            // If key > keyAtPos -> move lower to pos+1
            // Terminate when lower == higher
            while ( lower < higher )
            {
                pos = (lower + higher) / 2;
                comparison = comparator.compare( key, bTreeNode.keyAt( cursor, readKey, pos ) );
                if ( comparison <= 0 )
                {
                    higher = pos;
                }
                else
                {
                    lower = pos+1;
                }
            }
            if ( lower != higher )
            {
                throw new TreeInconsistencyException( "The binary search terminated in an unexpected way. " +
                        "Expected lower and higher to be equal but was " + "lower=" + lower + ", higher=" + higher );
            }
            pos = lower;

            hit = comparator.compare( key, bTreeNode.keyAt( cursor, readKey, pos ) ) == 0;
        }
        return searchResult( pos, hit );
    }

    private static int searchResult( int pos, boolean hit )
    {
        return (pos & POSITION_MASK) | ((hit ? 1 : 0) << 31);
    }

    /**
     * Extracts the position from a search result from {@link #search(PageCursor, TreeNode, Object, Object, int)}.
     *
     * @param searchResult search result from {@link #search(PageCursor, TreeNode, Object, Object, int)}.
     * @return position of the search result.
     */
    static int positionOf( int searchResult )
    {
        return searchResult & POSITION_MASK;
    }

    /**
     * Extracts whether or not the searched key was found from search result from
     * {@link #search(PageCursor, TreeNode, Object, Object, int)}.
     *
     * @param searchResult search result form {@link #search(PageCursor, TreeNode, Object, Object, int)}.
     * @return whether or not the searched key was found.
     */
    static boolean isHit( int searchResult )
    {
        return (searchResult & HIT_MASK) != 0;
    }
}
