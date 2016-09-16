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

public interface TreeNode
{
    void initializeLeaf( PageCursor cursor );

    void initializeInternal( PageCursor cursor );

    boolean isLeaf( PageCursor cursor );

    boolean isInternal( PageCursor cursor );

    int keyCount( PageCursor cursor );

    long rightSibling( PageCursor cursor );

    long leftSibling( PageCursor cursor );

    void setTypeLeaf( PageCursor cursor );

    void setTypeInternal( PageCursor cursor );

    void setKeyCount( PageCursor cursor, int count );

    void setRightSibling( PageCursor cursor, long rightSiblingId );

    void setLeftSibling( PageCursor cursor, long leftSiblingId );

    long[] keyAt( PageCursor cursor, long[] into, int pos );

    void setKeyAt( PageCursor cursor, long[] key, int pos );

    int keysFromTo( PageCursor cursor, int fromIncluding, int toExcluding, byte[] into );

    void setKeysAt( PageCursor cursor, byte[] keys, int pos, int length );

    long[] valueAt( PageCursor cursor, long[] value, int pos );

    void setValueAt( PageCursor cursor, long[] value, int pos );

    int valuesFromTo( PageCursor cursor, int fromIncluding, int toExcluding, byte[] into );

    void setValuesAt( PageCursor cursor, byte[] values, int pos, int length );

    long childAt( PageCursor cursor, int pos );

    void setChildAt( PageCursor cursor, long child, int pos );

    int childrenFromTo( PageCursor cursor, int fromIncluding, int toExcluding, byte[] into );

    void setChildrenAt( PageCursor cursor, byte[] children, int pos, int length );

    int internalMaxKeyCount();

    int leafMaxKeyCount();

    int keyOffset( int pos );

    int valueOffset( int pos );

    int childOffset( int pos );

    boolean isNode( long node );

    int keySize();

    int valueSize();

    int childSize();

    Comparator<long[]> keyComparator();
}
