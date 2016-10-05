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
package org.neo4j.index.bptree;

import java.io.IOException;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.Hit;
import org.neo4j.io.pagecache.PageCursor;

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
    private final Object order;

    // data structures for the current b-tree node
    private int pos;
    private int keyCount;
    private boolean currentContainsEnd;
    private boolean reread;

    SeekCursor( PageCursor leafCursor, KEY mutableKey, VALUE mutableValue, TreeNode<KEY,VALUE> bTreeNode,
            KEY fromInclusive, KEY toExclusive, Layout<KEY,VALUE> layout, int pos, Object order, int keyCount )
    {
        this.cursor = leafCursor;
        this.mutableKey = mutableKey;
        this.mutableValue = mutableValue;
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
        this.layout = layout;
        this.hit = new MutableHit<>( mutableKey, mutableValue );
        this.bTreeNode = bTreeNode;
        this.pos = pos;
        this.prevKey = layout.newKey();

        this.order = order;
        this.keyCount = keyCount;
        this.currentContainsEnd = layout.compare(
                bTreeNode.keyAt( cursor, mutableKey, keyCount - 1, order ), toExclusive ) >= 0;
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
                            bTreeNode.keyAt( cursor, mutableKey, keyCount - 1, order ), toExclusive ) >= 0;
                    bTreeNode.getOrder( cursor, order );
                    reread = false;
                }
                // There's a condition in here, choosing between go to next sibling or value,
                // this condition is mirrored outside the shouldRetry loop to act upon the data
                // which has by then been consistently read. No decision can be made in here directly.
                if ( pos >= keyCount )
                {
                    // Go to next sibling
                    rightSibling = bTreeNode.rightSibling( cursor );
                }
                else
                {
                    // Read the next value in this leaf
                    // TODO this is a hack for caching item order for unsorted tree node thingie
                    try
                    {
                        bTreeNode.keyAt( cursor, mutableKey, pos, order );
                    }
                    catch ( IllegalArgumentException e )
                    {
                        bTreeNode.getOrder( cursor, order );
                        bTreeNode.keyAt( cursor, mutableKey, pos, order );
                    }
                    bTreeNode.valueAt( cursor, mutableValue, pos, order );
                }
            }
            while ( reread = cursor.shouldRetry() );

            if ( pos >= keyCount )
            {
                if ( bTreeNode.isNode( rightSibling ) )
                {
                    if ( !cursor.next( rightSibling ) )
                    {
                        // TODO: Perhaps re-read if this happens instead?
                        return false;
                    }
                    bTreeNode.getOrder( cursor, order );
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
                            layout.compare( prevKey, mutableKey ) >= 0 )
                    {
                        // We've come across a bad read in the middle of a split
                        // This is outlined in IndexModifier, skip this value (it's fine)
                        // TODO: perhaps describe the circumstances here quickly as well
                        reread = true;
                        continue;
                    }

                    // A hit
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
