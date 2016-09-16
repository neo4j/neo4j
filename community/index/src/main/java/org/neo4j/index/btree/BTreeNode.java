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

import java.util.Comparator;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Methods to manipulate single node such as set and get header fields,
 * insert and fetch keys, values and children.
 *
 * DESIGN
 *
 * Using Separate design the internal nodes should look like
 *
 * # = empty space
 *
 * [                    HEADER               ]|[      KEYS     ]|[     CHILDREN      ]
 * [TYPE][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING]|[[KEY][KEY]...##]|[[CHILD][CHILD]...##]
 *  0     1         5             13            21
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 *
 * Calc offset for child i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_INTERNAL + i * SIZE_CHILD
 *
 *
 * Using Separate design the leaf nodes should look like
 *
 *
 * [                   HEADER                ]|[      KEYS     ]|[       VALUES      ]
 * [TYPE][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING]|[[KEY][KEY]...##]|[[VALUE][VALUE]...##]
 *  0     1         5             13            21
 *
 * Calc offset for key i (starting from 0)
 * HEADER_LENGTH + i * SIZE_KEY
 *
 * Calc offset for value i
 * HEADER_LENGTH + SIZE_KEY * MAX_KEY_COUNT_LEAF + i * SIZE_VALUE
 *
 */
public class BTreeNode implements TreeNode
{
    private static final int BYTE_POS_TYPE = 0;
    private static final int BYTE_POS_KEYCOUNT = 1;
    private static final int BYTE_POS_RIGHTSIBLING = 5;
    private static final int BYTE_POS_LEFTSIBLING = 13;
    private static final int HEADER_LENGTH = 21;

    private static final int SIZE_CHILD = 8;
    private static final int SIZE_KEY = 2 * 8;
    private static final int SIZE_VALUE = 2 * 8;

    private static final byte LEAF_FLAG = 1;
    private static final byte INTERNAL_FLAG = 0;
    private static final long NO_NODE_FLAG = -1l;

    private final int internalMaxKeyCount;
    private final int leafMaxKeyCount;

    private static final Comparator<long[]> KEY_COMPARATOR = ( left, right ) -> {
        if ( left.length != 2 || right.length != 2 )
        {
            throw new IllegalArgumentException( "Keys must have length 2 to be compared" );
        }
        int compareId = Long.compare( left[0], right[0] );
        return compareId != 0 ? compareId : Long.compare( left[1], right[1] );
    };

    public BTreeNode( int pageSize )
    {
        internalMaxKeyCount = Math.floorDiv( pageSize - (HEADER_LENGTH + SIZE_CHILD),
                SIZE_KEY + SIZE_CHILD);
        leafMaxKeyCount = Math.floorDiv( pageSize - HEADER_LENGTH,
                SIZE_KEY + SIZE_VALUE );
    }

    // ROUTINES

    @Override
    public void initializeLeaf( PageCursor cursor )
    {
        setTypeLeaf( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG );
        setLeftSibling( cursor, NO_NODE_FLAG );
    }

    @Override
    public void initializeInternal( PageCursor cursor )
    {
        setTypeInternal( cursor );
        setKeyCount( cursor, 0 );
        setRightSibling( cursor, NO_NODE_FLAG );
        setLeftSibling( cursor, NO_NODE_FLAG );
    }


    // HEADER METHODS

    @Override
    public boolean isLeaf( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == LEAF_FLAG;
    }

    @Override
    public boolean isInternal( PageCursor cursor )
    {
        return cursor.getByte( BYTE_POS_TYPE ) == INTERNAL_FLAG;
    }

    @Override
    public int keyCount( PageCursor cursor )
    {
        return cursor.getInt( BYTE_POS_KEYCOUNT );
    }

    @Override
    public long rightSibling( PageCursor cursor )
    {
        return cursor.getLong( BYTE_POS_RIGHTSIBLING );
    }

    @Override
    public long leftSibling( PageCursor cursor )
    {
        return cursor.getLong( BYTE_POS_LEFTSIBLING );
    }

    @Override
    public void setTypeLeaf( PageCursor cursor )
    {
        cursor.putByte( BYTE_POS_TYPE, LEAF_FLAG );
    }

    @Override
    public void setTypeInternal( PageCursor cursor )
    {
        cursor.putByte( BYTE_POS_TYPE, INTERNAL_FLAG );
    }

    @Override
    public void setKeyCount( PageCursor cursor, int count )
    {
        cursor.putInt( BYTE_POS_KEYCOUNT, count );
    }

    @Override
    public void setRightSibling( PageCursor cursor, long rightSiblingId )
    {
        cursor.putLong( BYTE_POS_RIGHTSIBLING, rightSiblingId );
    }

    @Override
    public void setLeftSibling( PageCursor cursor, long leftSiblingId )
    {
        cursor.putLong( BYTE_POS_LEFTSIBLING, leftSiblingId );
    }

    // BODY METHODS

    @Override
    public long[] keyAt( PageCursor cursor, long[] into, int pos )
    {
        cursor.setOffset( keyOffset( pos ) );
        into[0] = cursor.getLong();
        into[1] = cursor.getLong();
        return into;
    }

    @Override
    public void setKeyAt( PageCursor cursor, long[] key, int pos )
    {
        cursor.setOffset( keyOffset( pos ) );
        cursor.putLong( key[0] );
        cursor.putLong( key[1] );
    }

    @Override
    public int keysFromTo( PageCursor cursor, int fromIncluding, int toExcluding, byte[] into )
    {
        int length = (toExcluding - fromIncluding) * SIZE_KEY;
        cursor.setOffset( keyOffset( fromIncluding ) );
        cursor.getBytes( into, 0, length );
        return length;
    }

    @Override
    public void setKeysAt( PageCursor cursor, byte[] keys, int pos, int length )
    {
        cursor.setOffset( keyOffset( pos ) );
        cursor.putBytes( keys, 0, length );
    }

    @Override
    public long[] valueAt( PageCursor cursor, long[] value, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        value[0] = cursor.getLong();
        value[1] = cursor.getLong();
        return value;
    }

    @Override
    public void setValueAt( PageCursor cursor, long[] value, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        cursor.putLong( value[0] );
        cursor.putLong( value[1] );
    }

    @Override
    public int valuesFromTo( PageCursor cursor, int fromIncluding, int toExcluding, byte[] into )
    {
        int length = (toExcluding - fromIncluding) * SIZE_VALUE;
        cursor.setOffset( valueOffset( fromIncluding ) );
        cursor.getBytes( into, 0, length );
        return length;
    }

    @Override
    public void setValuesAt( PageCursor cursor, byte[] values, int pos, int length )
    {
        cursor.setOffset( valueOffset( pos ) );
        cursor.putBytes( values, 0, length );
    }

    @Override
    public long childAt( PageCursor cursor, int pos )
    {
        return cursor.getLong( childOffset( pos ) );
    }

    @Override
    public void setChildAt( PageCursor cursor, long child, int pos )
    {
        cursor.putLong( childOffset( pos ), child );
    }

    @Override
    public int childrenFromTo( PageCursor cursor, int fromIncluding, int toExcluding, byte[] into )
    {
        int length = (toExcluding - fromIncluding) * SIZE_CHILD;
        cursor.setOffset( childOffset( fromIncluding ) );
        cursor.getBytes( into, 0, length );
        return length;
    }

    @Override
    public void setChildrenAt( PageCursor cursor, byte[] children, int pos, int length )
    {
        cursor.setOffset( childOffset( pos ) );
        cursor.putBytes( children, 0, length );
    }

    @Override
    public int internalMaxKeyCount()
    {
        return internalMaxKeyCount;
    }

    @Override
    public int leafMaxKeyCount()
    {
        return leafMaxKeyCount;
    }

    // HELPERS

    @Override
    public int keyOffset( int pos )
    {
        return HEADER_LENGTH + pos * SIZE_KEY;
    }

    @Override
    public int valueOffset( int pos )
    {
        return HEADER_LENGTH + leafMaxKeyCount * SIZE_KEY + pos * SIZE_VALUE;
    }

    @Override
    public int childOffset( int pos )
    {
        return HEADER_LENGTH + internalMaxKeyCount * SIZE_KEY + pos * SIZE_CHILD;
    }

    @Override
    public boolean isNode( long node )
    {
        return node != NO_NODE_FLAG;
    }

    @Override
    public int keySize()
    {
        return SIZE_KEY;
    }

    @Override
    public int valueSize()
    {
        return SIZE_VALUE;
    }

    @Override
    public int childSize()
    {
        return SIZE_CHILD;
    }

    @Override
    public Comparator<long[]> keyComparator()
    {
        return KEY_COMPARATOR;
    }
}
