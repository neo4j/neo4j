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
package org.neo4j.index.btree;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Used to search for keys in internal and leaf node.
 */
public class IndexSearch
{
    /**
     * Leaves cursor on same page as when called. No guaranties on offset.
     *
     * Search for keyAtPos such that key <= keyAtPos. Return first position of keyAtPos (not offset),
     * or key count if no such key exist.
     *
     * On insert, key should be inserted at pos.
     * On seek in internal, child at pos should be followed from internal node.
     * On seek in leaf, value at pos is correct if keyAtPos is equal to key.
     *
     * Simple implementation, linear search.
     *
     * //TODO: Implement binary search
     *
     * @param cursor    {@link PageCursor} pinned to page with node (internal or leaf does not matter)
     * @param bTreeNode {@link BTreeNode} to use for node interpretation
     * @param key       long[] of length 2 where key[0] is id and key[1] is property value
     * @return          first position i for which Node.KEY_COMPARATOR.compare( key, Node.keyAt( i ) <= 0;
     */
    public static int search( PageCursor cursor, BTreeNode bTreeNode, long[] key )
    {
        int keyCount = bTreeNode.keyCount( cursor );

        if ( keyCount == 0 )
        {
            return 0;
        }

        int lower = 0;
        int higher = keyCount-1;
        int pos;
        long[] readKey = new long[2];

        // Compare key with lower and higher and sort out special cases
        if ( BTreeNode.KEY_COMPARATOR.compare( key, bTreeNode.keyAt( cursor, readKey, higher ) ) > 0 )
        {
            pos = keyCount;
        }
        else if ( BTreeNode.KEY_COMPARATOR.compare( key, bTreeNode.keyAt( cursor, readKey, lower ) ) < 0 )
        {
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
                if ( BTreeNode.KEY_COMPARATOR.compare( key, bTreeNode.keyAt( cursor, readKey, pos ) ) <= 0 )
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
                throw new IllegalStateException( "Something went terribly wrong. The binary search terminated in an " +
                                                 "unexpected way." );
            }
            pos = lower;
        }
        return pos;
    }
}
