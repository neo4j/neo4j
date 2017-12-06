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

import java.util.StringJoiner;

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.BYTE_SIZE_KEY_SIZE;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.BYTE_SIZE_OFFSET;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.BYTE_SIZE_TOTAL_OVERHEAD;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.BYTE_SIZE_VALUE_SIZE;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.hasTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyOffset;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyOffset;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readValueSize;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

public class TreeNodeDynamicSize<KEY, VALUE> extends TreeNode<KEY,VALUE>
{
    private static final int BYTE_POS_ALLOCSPACE = BASE_HEADER_LENGTH;
    private static final int HEADER_LENGTH_DYNAMIC = BYTE_POS_ALLOCSPACE + BYTE_SIZE_OFFSET;

    private static final int LEAST_NUMBER_OF_ENTRIES_PER_PAGE = 2;
    private static final int MINIMUM_ENTRY_SIZE_CAP = Long.SIZE;
    private final int keyValueSizeCap;

    TreeNodeDynamicSize( int pageSize, Layout<KEY,VALUE> layout )
    {
        super( pageSize, layout );

        keyValueSizeCap = (pageSize - HEADER_LENGTH_DYNAMIC) / LEAST_NUMBER_OF_ENTRIES_PER_PAGE - BYTE_SIZE_TOTAL_OVERHEAD;

        if ( keyValueSizeCap < MINIMUM_ENTRY_SIZE_CAP )
        {
            throw new MetadataMismatchException(
                    "We need to fit at least %d key-value entries per page in leaves. To do that a key-value entry can be at most %dB " +
                            "with current page size of %dB. We require this cap to be at least %dB.",
                    LEAST_NUMBER_OF_ENTRIES_PER_PAGE, keyValueSizeCap, pageSize, Long.SIZE );
        }
    }

    @Override
    void writeAdditionalHeader( PageCursor cursor )
    {
        setAllocSpace( cursor, pageSize );
    }

    private void setAllocSpace( PageCursor cursor, int allocSpace )
    {
        cursor.setOffset( BYTE_POS_ALLOCSPACE );
        putKeyOffset( cursor, allocSpace );
    }

    int getAllocSpace( PageCursor cursor )
    {
        cursor.setOffset( BYTE_POS_ALLOCSPACE );
        return readKeyOffset( cursor );
    }

    @Override
    KEY keyAt( PageCursor cursor, KEY into, int pos, Type type )
    {
        placeCursorAtActualKey( cursor, pos, type );

        // Read key
        int keySize = readKeySize( cursor );
        if ( keySize > keyValueSizeCap || keySize < 0 )
        {
            cursor.setCursorException( format( "Read unreliable key, keySize=%d, keyValueSizeCap=%d, keyHasTombstone=%b",
                    keySize, keyValueSizeCap, hasTombstone( keySize ) ) );
        }
        if ( type == LEAF )
        {
            progressCursor( cursor, DynamicSizeUtil.BYTE_SIZE_VALUE_SIZE );
        }
        layout.readKey( cursor, into, keySize );
        return into;
    }

    private void placeCursorAtActualKey( PageCursor cursor, int pos, Type type )
    {
        // Set cursor to correct place in offset array
        int keyPosOffset = keyPosOffset( pos, type );
        cursor.setOffset( keyPosOffset );

        // Read actual offset to key
        int keyOffset = readKeyOffset( cursor );

        // Verify offset is reasonable
        if ( keyOffset > pageSize )
        {
            cursor.setCursorException( "Tried to read key on offset " + keyOffset + ". Page size is " + pageSize );
        }

        // Set cursor to actual offset
        cursor.setOffset( keyOffset );
    }

    private int keyPosOffset( int pos, Type type )
    {
        if ( type == LEAF )
        {
            return keyPosOffsetLeaf( pos );
        }
        else
        {
            return keyPosOffsetInternal( pos );
        }
    }

    private int keyPosOffsetLeaf( int pos )
    {
        return HEADER_LENGTH_DYNAMIC + pos * BYTE_SIZE_OFFSET;
    }

    private int keyPosOffsetInternal( int pos )
    {
        // header + childPointer + pos * (keyPosOffsetSize + childPointer)
        return HEADER_LENGTH_DYNAMIC + SIZE_PAGE_REFERENCE + pos * keyChildSize();
    }

    @Override
    void insertKeyAndRightChildAt( PageCursor cursor, KEY key, long child, int pos, int keyCount, long stableGeneration,
            long unstableGeneration )
    {
        // Where to write key?
        int currentKeyOffset = getAllocSpace( cursor );
        int keySize = layout.keySize( key );
        int newKeyOffset = currentKeyOffset - BYTE_SIZE_KEY_SIZE - keySize;

        // Write key
        cursor.setOffset( newKeyOffset );
        putKeySize( cursor, keySize );
        layout.writeKey( cursor, key );

        // Update alloc space
        setAllocSpace( cursor, newKeyOffset );

        // Write to offset array
        insertSlotsAt( cursor, pos, 1, keyCount, keyPosOffsetInternal( 0 ), keyChildSize() );
        cursor.setOffset( keyPosOffsetInternal( pos ) );
        putKeyOffset( cursor, newKeyOffset );
        writeChild( cursor, child, stableGeneration, unstableGeneration );
    }

    @Override
    void insertKeyValueAt( PageCursor cursor, KEY key, VALUE value, int pos, int keyCount )
    {
        // Where to write key?
        int currentKeyValueOffset = getAllocSpace( cursor );
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        int newKeyValueOffset = currentKeyValueOffset - BYTE_SIZE_KEY_SIZE - BYTE_SIZE_VALUE_SIZE - keySize - valueSize;

        // Write key and value
        cursor.setOffset( newKeyValueOffset );
        putKeySize( cursor, keySize );
        putValueSize( cursor, valueSize );
        layout.writeKey( cursor, key );
        layout.writeValue( cursor, value );

        // Update alloc space
        setAllocSpace( cursor, newKeyValueOffset );

        // Write to offset array
        insertSlotsAt( cursor, pos, 1, keyCount, keyPosOffsetLeaf( 0 ), keySize() );
        cursor.setOffset( keyPosOffsetLeaf( pos ) );
        putKeyOffset( cursor, newKeyValueOffset );
    }

    @Override
    void removeKeyValueAt( PageCursor cursor, int pos, int keyCount )
    {
        // Kill actual key
        placeCursorAtActualKey( cursor, pos, LEAF );
        DynamicSizeUtil.putTombstone( cursor );

        // Remove from offset array
        removeSlotAt( cursor, pos, keyCount, keyPosOffsetLeaf( 0 ), keySize() );
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
    VALUE valueAt( PageCursor cursor, VALUE into, int pos )
    {
        placeCursorAtActualKey( cursor, pos, LEAF );

        // Read value
        int keySize = readKeySize( cursor );
        int valueSize = readValueSize( cursor );
        if ( valueSize > keyValueSizeCap )
        {
            cursor.setCursorException( format( "Read unreliable key, value size greater than cap: keySize=%d, keyValueSizeCap=%d",
                    valueSize, keyValueSizeCap ) );
        }
        progressCursor( cursor, keySize );
        layout.readValue( cursor, into, valueSize );
        return into;
    }

    @Override
    boolean setValueAt( PageCursor cursor, VALUE value, int pos )
    {
        placeCursorAtActualKey( cursor, pos, LEAF );

        int keySize = DynamicSizeUtil.readKeyOffset( cursor );
        int oldValueSize = DynamicSizeUtil.readValueSize( cursor );
        int newValueSize = layout.valueSize( value );
        if ( oldValueSize == newValueSize )
        {
            // Fine we can just overwrite
            progressCursor( cursor, keySize );
            layout.writeValue( cursor, value );
            return true;
        }
        return false;
    }

    private void progressCursor( PageCursor cursor, int delta )
    {
        cursor.setOffset( cursor.getOffset() + delta );
    }

    @Override
    long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( childOffset( pos ) );
        return read( cursor, stableGeneration, unstableGeneration, pos );
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
        // Child pointer to the left of key at pos
        return keyPosOffsetInternal( pos ) - SIZE_PAGE_REFERENCE;
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

    private int keyChildSize()
    {
        return BYTE_SIZE_OFFSET + SIZE_PAGE_REFERENCE;
    }

    private int keySize()
    {
        return BYTE_SIZE_OFFSET;
    }

    @Override
    public String toString()
    {
        return "TreeNodeDynamicSize[pageSize:" + pageSize + ", keyValueSizeCap:" + keyValueSizeCap + "]";
    }

    @SuppressWarnings( "unused" )
    void printNode( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        // [header] <- dont care
        // LEAF:     [allocSpace=][child0,key0*,child1,...][keySize|key][keySize|key]
        // INTERNAL: [allocSpace=][key0*,key1*,...][offset|keySize|valueSize|key][keySize|valueSize|key]

        Type type = isInternal( cursor ) ? INTERNAL : LEAF;

        // HEADER
        int allocSpace = getAllocSpace( cursor );
        String additionalHeader = "[allocSpace=" + allocSpace + "]";

        // OFFSET ARRAY
        String offsetArray = readOffsetArray( cursor, stableGeneration, unstableGeneration, type );

        // KEYS
        KEY readKey = layout.newKey();
        VALUE readValue = layout.newValue();
        StringJoiner keys = new StringJoiner( "][", "[", "]" );
        cursor.setOffset( allocSpace );
        while ( cursor.getOffset() < cursor.getCurrentPageSize() )
        {
            StringJoiner singleKey = new StringJoiner( "|" );
            singleKey.add( Integer.toString( cursor.getOffset() ) );
            int keySize = readKeySize( cursor );
            int valueSize = 0;
            if ( type == LEAF )
            {
                valueSize = readValueSize( cursor );
            }
            layout.readKey( cursor, readKey, keySize );
            if ( type == LEAF )
            {
                layout.readValue( cursor, readValue, valueSize );
            }
            singleKey.add( Integer.toString( keySize ) );
            if ( type == LEAF )
            {
                singleKey.add( Integer.toString( valueSize ) );
            }
            singleKey.add( readKey.toString() );
            if ( type == LEAF )
            {
                singleKey.add( readValue.toString() );
            }
            keys.add( singleKey.toString() );
        }

        System.out.println( additionalHeader + offsetArray + keys );
    }

    private String readOffsetArray( PageCursor cursor, long stableGeneration, long unstableGeneration, Type type )
    {
        int keyCount = keyCount( cursor );
        StringJoiner offsetArray = new StringJoiner( ",", "[", "]" );
        for ( int i = 0; i < keyCount; i++ )
        {
            if ( type == INTERNAL )
            {
                long childPointer = GenerationSafePointerPair.pointer( childAt( cursor, i, stableGeneration, unstableGeneration ) );
                offsetArray.add( "/" + Long.toString( childPointer ) + "\\" );
            }
            cursor.setOffset( keyPosOffset( i, type ) );
            offsetArray.add( Integer.toString( DynamicSizeUtil.readKeyOffset( cursor ) ) );
        }
        if ( type == INTERNAL )
        {
            long childPointer = GenerationSafePointerPair.pointer( childAt( cursor, keyCount, stableGeneration, unstableGeneration ) );
            offsetArray.add( "/" + Long.toString( childPointer ) + "\\" );
        }
        return offsetArray.toString();
    }
}
