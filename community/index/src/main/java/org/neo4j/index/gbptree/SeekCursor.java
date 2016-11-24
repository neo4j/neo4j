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
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link RawCursor} over tree leaves, making keys/values accessible to user. Given a starting leaf
 * and key range this cursor traverses each leaf and its right siblings as long as visited keys are within
 * key range. Each visited key within the key range can be accessible using {@link #get()}.
 * The key/value instances provided by {@link Hit} instance are mutable and overwritten with new values
 * for every call to {@link #next()} so user cannot keep references to key/value instances, expecting them
 * to keep their values intact.
 * <p>
 * Concurrent writes can happen in the visited nodes and tree structure may change. This implementation
 * guards for that by re-reading if change happens underneath, but will not provide a consistent view of
 * the data as it were when the seek starts, i.e. doesn't support MVCC-style.
 *
 * Implementation note: there are assumptions that keys are unique in the tree.
 */
class SeekCursor<KEY,VALUE> implements RawCursor<Hit<KEY,VALUE>,IOException>
{
    private final PageCursor cursor;
    private final KEY mutableKey;
    private final VALUE mutableValue;
    private final KEY fromInclusive;
    private final KEY toExclusive;
    private final Layout<KEY,VALUE> layout;
    private final MutableHit<KEY,VALUE> hit;
    private final TreeNode<KEY,VALUE> bTreeNode;
    private final KEY prevKey;
    private boolean first = true;

    // data structures for the current b-tree node
    private int pos;
    private int keyCount;
    private boolean currentContainsEnd;
    private boolean reread;
    private boolean resetPosition;
    private final long stableGeneration;
    private final long unstableGeneration;

    SeekCursor( PageCursor leafCursor, KEY mutableKey, VALUE mutableValue, TreeNode<KEY,VALUE> bTreeNode,
            KEY fromInclusive, KEY toExclusive, Layout<KEY,VALUE> layout,
            long stableGeneration, long unstableGeneration,
            int firstPosToRead, int keyCount )
    {
        this.cursor = leafCursor;
        this.mutableKey = mutableKey;
        this.mutableValue = mutableValue;
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
        this.layout = layout;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.hit = new MutableHit<>( mutableKey, mutableValue );
        this.bTreeNode = bTreeNode;
        this.pos = firstPosToRead - 1;
        this.prevKey = layout.newKey();

        this.keyCount = keyCount;
        this.currentContainsEnd = layout.compare(
                bTreeNode.keyAt( cursor, mutableKey, keyCount - 1 ), toExclusive ) >= 0;
    }

    @Override
    public Hit<KEY,VALUE> get()
    {
        return hit;
    }

    @Override
    public boolean next() throws IOException
    {
        while ( true )
        {
            pos++;
            long rightSibling = -1; // initialized to satisfy the compiler
            do
            {
                if ( reread )
                {
                    keyCount = bTreeNode.keyCount( cursor );
                    currentContainsEnd = layout.compare(
                            bTreeNode.keyAt( cursor, mutableKey, keyCount - 1 ), toExclusive ) >= 0;

                    // Keys could have been moved to the left so we need to make sure we are not missing any keys by
                    // moving position back until we find previously returned key
                    if ( resetPosition )
                    {
                        int searchResult = KeySearch.search( cursor, bTreeNode, prevKey, mutableKey, keyCount );
                        pos = KeySearch.positionOf( searchResult );
                        resetPosition = false;
                    }
                    reread = false;
                }
                // There's a condition in here, choosing between go to next sibling or value,
                // this condition is mirrored outside the shouldRetry loop to act upon the data
                // which has by then been consistently read. No decision can be made in here directly.
                if ( pos >= keyCount )
                {
                    // Go to next sibling
                    rightSibling = bTreeNode.rightSibling( cursor, stableGeneration, unstableGeneration );
                }
                else
                {
                    // Read the next value in this leaf
                    bTreeNode.keyAt( cursor, mutableKey, pos );
                    bTreeNode.valueAt( cursor, mutableValue, pos );
                }
            }
            while ( resetPosition = reread = cursor.shouldRetry() );
            if ( cursor.checkAndClearBoundsFlag() )
            {
                throw new IllegalStateException( "Read out of bounds" );
            }

            if ( pos >= keyCount )
            {
                if ( bTreeNode.isNode( rightSibling ) )
                {
                    // TODO: Check if rightSibling is within expected range before calling next.
                    // TODO: Possibly by getting highest expected from IdProvider
                    bTreeNode.goTo( cursor, rightSibling, stableGeneration, unstableGeneration );
                    pos = -1;
                    reread = true;
                    continue; // in the outer loop, with the position reset to the beginning of the right sibling
                }
            }
            else
            {
                if ( !currentContainsEnd || layout.compare( mutableKey, toExclusive ) < 0 )
                {
                    if ( layout.compare( mutableKey, fromInclusive ) < 0 ||
                         ( !first && layout.compare( prevKey, mutableKey ) >= 0 ) )
                    {
                        // We've come across a bad read in the middle of a split
                        // This is outlined in InternalTreeLogic, skip this value (it's fine)
                        reread = true;
                        continue;
                    }

                    // A hit
                    if ( first )
                    {
                        first = false;
                    }
                    layout.copyKey( mutableKey, prevKey );
                    return true;
                }
                // else we've come too far and so this means the end of the result set
            }
            return false;
        }
    }

    @Override
    public void close()
    {
        cursor.close();
    }
}
