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

import org.neo4j.io.pagecache.PageCursor;

public class TreeNodeDynamicSize<KEY, VALUE> extends TreeNode<KEY,VALUE>
{
    TreeNodeDynamicSize( int pageSize, Layout<KEY,VALUE> layout )
    {
        super( pageSize, layout );
    }

    @Override
    KEY keyAt( PageCursor cursor, KEY into, int pos, Type type )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void insertKeyAndRightChildAt( PageCursor cursor, KEY key, long child, int pos, int keyCount, long stableGeneration,
            long unstableGeneration )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void insertKeyValueAt( PageCursor cursor, KEY key, VALUE value, int pos, int keyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void removeKeyAt( PageCursor cursor, int pos, int keyCount, Type type )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void removeKeyValueAt( PageCursor cursor, int pos, int keyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void removeKeyAndRightChildAt( PageCursor cursor, int keyPos, int keyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void removeKeyAndLeftChildAt( PageCursor cursor, int keyPos, int keyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void setKeyAt( PageCursor cursor, KEY key, int pos, Type type )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    VALUE valueAt( PageCursor cursor, VALUE value, int pos )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void setValueAt( PageCursor cursor, VALUE value, int pos )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    int internalMaxKeyCount()
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    int leafMaxKeyCount()
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    boolean reasonableKeyCount( int keyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    int childOffset( int pos )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    String additionalToString()
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    boolean internalOverflow( int keyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    boolean leafOverflow( int keyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    boolean leafUnderflow( int keyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    boolean canRebalanceLeaves( int leftKeyCount, int rightKeyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    boolean canMergeLeaves( int leftKeyCount, int rightKeyCount )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void doSplitLeaf( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int insertPos, KEY newKey,
            VALUE newValue, int middlePos )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void doSplitInternal( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int insertPos, KEY newKey,
            long newRightChild, int middlePos, long stableGeneration, long unstableGeneration )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    void moveKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount,
            int fromPosInLeftNode )
    {
        throw new UnsupportedOperationException( "Implement me" );
    }
}
