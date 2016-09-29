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

import org.neo4j.cursor.Cursor;
import org.neo4j.index.BTreeHit;
import org.neo4j.io.pagecache.PageCursor;

class SeekCursor<KEY,VALUE> implements Cursor<BTreeHit<KEY,VALUE>>
{
    private final PageCursor cursor;
    private final KEY mutableKey;
    private final VALUE mutableValue;
    private final KEY fromInclusive;
    private final KEY toExclusive;
    private final Comparator<KEY> keyComparator;
    private final MutableBTreeHit<KEY,VALUE> hit;
    private final TreeNode<KEY,VALUE> bTreeNode;

    // data structures for the current b-tree node
    private int keyCount;
    private int pos;

    SeekCursor( PageCursor leafCursor, KEY mutableKey, VALUE mutableValue, TreeNode<KEY,VALUE> bTreeNode,
            KEY fromInclusive, KEY toExclusive, Comparator<KEY> keyComparator )
    {
        this.cursor = leafCursor;
        this.mutableKey = mutableKey;
        this.mutableValue = mutableValue;
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
        this.keyComparator = keyComparator;
        this.hit = new MutableBTreeHit<>( mutableKey, mutableValue );
        this.bTreeNode = bTreeNode;
        initTreeNode( true );
    }

    private void gotoTreeNode( long id )
    {
        try
        {
            cursor.next( id );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        cursor.setOffset( 0 );
        initTreeNode( false );
    }

    private void initTreeNode( boolean firstLeaf )
    {
        // Find the left-most key within from-range
        keyCount = bTreeNode.keyCount( cursor );
        pos = 0;
        if ( firstLeaf )
        {
            bTreeNode.keyAt( cursor, mutableKey, pos );
            while ( pos < keyCount && keyComparator.compare( mutableKey, fromInclusive ) < 0 )
            {
                pos++;
                bTreeNode.keyAt( cursor, mutableKey, pos );
            }
        }
        pos--;
    }

    @Override
    public BTreeHit<KEY,VALUE> get()
    {
        return hit;
    }

    @Override
    public boolean next()
    {
        while ( true )
        {
            pos++;
            if ( pos >= keyCount )
            {
                long rightSibling = bTreeNode.rightSibling( cursor );
                if ( bTreeNode.isNode( rightSibling ) )
                {
                    gotoTreeNode( rightSibling );
                    continue;
                }
                return false;
            }

            // Go to the next one, so that next call to next() gets it
            bTreeNode.keyAt( cursor, mutableKey, pos );
            if ( keyComparator.compare( mutableKey, toExclusive ) < 0 )
            {
                // A hit
                bTreeNode.valueAt( cursor, mutableValue, pos );
                return true;
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
