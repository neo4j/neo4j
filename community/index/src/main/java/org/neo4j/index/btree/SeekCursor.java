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

import java.io.IOException;
import java.util.Comparator;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.BTreeHit;
import org.neo4j.io.pagecache.PageCursor;

class SeekCursor<KEY,VALUE> implements RawCursor<BTreeHit<KEY,VALUE>,IOException>
{
    private final PageCursor cursor;
    private final KEY mutableKey;
    private final VALUE mutableValue;
    private final KEY fromInclusive;
    private final KEY toExclusive;
    private final Comparator<KEY> keyComparator;
    private final MutableBTreeHit<KEY,VALUE> hit;
    private final TreeNode<KEY,VALUE> bTreeNode;
    private long prevNode;

    // data structures for the current b-tree node
    private int pos;

    SeekCursor( PageCursor leafCursor, KEY mutableKey, VALUE mutableValue, TreeNode<KEY,VALUE> bTreeNode,
            KEY fromInclusive, KEY toExclusive, Comparator<KEY> keyComparator, int pos ) throws IOException
    {
        this.cursor = leafCursor;
        this.mutableKey = mutableKey;
        this.mutableValue = mutableValue;
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
        this.keyComparator = keyComparator;
        this.hit = new MutableBTreeHit<>( mutableKey, mutableValue );
        this.bTreeNode = bTreeNode;
        this.pos = pos;
    }

    private boolean gotoTreeNode( long id ) throws IOException
    {
        prevNode = cursor.getCurrentPageId();
        if ( !cursor.next( id ) )
        {
            return false;
        }
        pos = -1;
        return true;
    }

    @Override
    public BTreeHit<KEY,VALUE> get()
    {
        return hit;
    }

    @Override
    public boolean next() throws IOException
    {
        while ( true )
        {
            pos++;
            long rightSibling;
            boolean hit;
            int keyCount;
            do
            {
                rightSibling = -1;
                hit = false;

                // There's a condition in here, choosing between go to next sibling or value,
                // this condition is mirrored outside the shouldRetry loop to act upon the data
                // which has by then been consistently read.
                keyCount = bTreeNode.keyCount( cursor );
                if ( pos >= keyCount )
                {
                    // Go to next sibling
                    rightSibling = bTreeNode.rightSibling( cursor );
                }
                else
                {
                    // Go to the next value in this leaf
                    bTreeNode.keyAt( cursor, mutableKey, pos );
                    if ( keyComparator.compare( mutableKey, toExclusive ) < 0 )
                    {
                        // A hit
                        bTreeNode.valueAt( cursor, mutableValue, pos );
                        hit = true;
                    }
                }
            }
            while ( cursor.shouldRetry() );
            // TODO: what should we do with an out-of-bounds access here?
            cursor.checkAndClearBoundsFlag();
            cursor.checkAndClearCursorException();

            if ( pos >= keyCount )
            {
                if ( bTreeNode.isNode( rightSibling ) )
                {
                    if ( !gotoTreeNode( rightSibling ) )
                    {
                        // TODO: Perhaps re-read if this happens instead?
                        return false;
                    }
                    continue; // in the outer loop
                }
            }
            else
            {
                // if hit == false it means we came too far and so the whole seek should end
                // if hit == true it means we read a value which should be included in the result
                if ( hit )
                {
                    return true;
                }
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
