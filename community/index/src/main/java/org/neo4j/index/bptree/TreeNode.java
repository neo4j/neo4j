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

import java.util.Comparator;
import java.util.function.Consumer;

import org.neo4j.io.pagecache.PageCursor;

public interface TreeNode<KEY,VALUE>
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

    Object newOrder();

    void getOrder( PageCursor cursor, Object into );

    KEY keyAt( PageCursor cursor, KEY into, int pos, Object order );

    void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount, Object order, byte[] tmp );

    void removeKeyAt( PageCursor cursor, int pos, Object order, byte[] tmp );

    // no setKeyAt since we only insert or remove keys, when they're there they're not changed

    VALUE valueAt( PageCursor cursor, VALUE value, int pos, Object order );

    void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount, Object order, byte[] tmp );

    void removeValueAt( PageCursor cursor, int pos, Object order, byte[] tmp );

    void setValueAt( PageCursor cursor, VALUE value, int pos, Object order );

    long childAt( PageCursor cursor, int pos, Object order );

    void insertChildAt( PageCursor cursor, long child, int pos, int keyCount, Object order, byte[] tmp );

    void setChildAt( PageCursor cursor, long child, int pos, Object order );

    // no removeChildAt since we don't do merge and stuff yet

    int internalMaxKeyCount();

    int leafMaxKeyCount();

    int keyOffset( int pos );

    int valueOffset( int pos );

    int childOffset( int pos );

    boolean isNode( long node );

    int keySize();

    int valueSize();

    int childSize();

    Comparator<KEY> keyComparator();

    // TODO: incremental methods towards V2

    int readKeysWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, byte[] into );

    int readValuesWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, byte[] into );

    int readChildrenWithInsertRecordInPosition( PageCursor cursor, Consumer<PageCursor> newRecordWriter,
            int insertPosition, int totalNumberOfRecords, byte[] into );

    void writeKeys( PageCursor cursor, byte[] source, int sourcePos, int targetPos, int count );

    void writeValues( PageCursor cursor, byte[] source, int sourcePos, int targetPos, int count );

    void writeChildren( PageCursor cursor, byte[] source, int sourcePos, int targetPos, int count );

    void writeChild( PageCursor cursor, long child );
}
