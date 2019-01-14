/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.Arrays;
import java.util.StringJoiner;

import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SIZE_KEY_SIZE;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SIZE_OFFSET;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SIZE_TOTAL_OVERHEAD;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.SIZE_VALUE_SIZE;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getOverhead;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyOffset;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putTombstone;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyOffset;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyValueSize;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.putUnsignedShort;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;

/**
 * # = empty space
 * K* = offset to key or key and value
 *
 * LEAF
 * [                                   HEADER   86B                                                   ]|[KEY_OFFSETS]##########[KEYS_VALUES]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR][ALLOCOFFSET][DEADSPACE]|[K0*,K1*,K2*]->      <-[KV0,KV2,KV1]
 *  0         1     2           6         10            34           58         82           84          86
 *
 *  INTERNAL
 * [                                   HEADER   86B                                                   ]|[  KEY_OFFSET_CHILDREN  ]######[  KEYS  ]
 * [NODETYPE][TYPE][GENERATION][KEYCOUNT][RIGHTSIBLING][LEFTSIBLING][SUCCESSOR][ALLOCOFFSET][DEADSPACE]|[C0,K0*,C1,K1*,C2,K2*,C3]->  <-[K2,K0,K1]
 *  0         1     2           6         10            34           58         82           84          86
 *
 * See {@link DynamicSizeUtil} for more detailed layout for individual offset array entries and key / key_value entries.
 */
public class TreeNodeDynamicSize<KEY, VALUE> extends TreeNode<KEY,VALUE>
{
    static final byte FORMAT_IDENTIFIER = 3;
    static final byte FORMAT_VERSION = 0;

    /**
     * Concepts
     * Total space - The space available for data (pageSize - headerSize)
     * Active space - Space currently occupied by active data (not including dead keys)
     * Dead space - Space currently occupied by dead data that could be reclaimed by defragment
     * Alloc offset - Exact offset to leftmost key and thus the end of alloc space
     * Alloc space - The available space between offset array and data space
     *
     * TotalSpace  |----------------------------------------|
     * ActiveSpace |-----------|   +    |---------|  + |----|
     * DeadSpace                                  |----|
     * AllocSpace              |--------|
     * AllocOffset                      v
     *     [Header][OffsetArray]........[_________,XXXX,____] (_ = alive key, X = dead key)
     */
    private static final int BYTE_POS_ALLOCOFFSET = BASE_HEADER_LENGTH;
    private static final int BYTE_POS_DEADSPACE = BYTE_POS_ALLOCOFFSET + bytesPageOffset();
    private static final int HEADER_LENGTH_DYNAMIC = BYTE_POS_DEADSPACE + bytesPageOffset();

    private static final int LEAST_NUMBER_OF_ENTRIES_PER_PAGE = 2;
    private static final int MINIMUM_ENTRY_SIZE_CAP = Long.SIZE;
    private final int keyValueSizeCap;
    private final PrimitiveIntStack deadKeysOffset = new PrimitiveIntStack();
    private final PrimitiveIntStack aliveKeysOffset = new PrimitiveIntStack();
    private final int maxKeyCount = pageSize / (bytesKeyOffset() + SIZE_KEY_SIZE + SIZE_VALUE_SIZE);
    private final int[] oldOffset = new int[maxKeyCount];
    private final int[] newOffset = new int[maxKeyCount];
    private final int totalSpace;
    private final int halfSpace;

    TreeNodeDynamicSize( int pageSize, Layout<KEY,VALUE> layout )
    {
        super( pageSize, layout );
        totalSpace = pageSize - HEADER_LENGTH_DYNAMIC;
        halfSpace = totalSpace / 2;
        keyValueSizeCap = keyValueSizeCapFromPageSize( pageSize );

        if ( keyValueSizeCap < MINIMUM_ENTRY_SIZE_CAP )
        {
            throw new MetadataMismatchException(
                    "We need to fit at least %d key-value entries per page in leaves. To do that a key-value entry can be at most %dB " +
                            "with current page size of %dB. We require this cap to be at least %dB.",
                    LEAST_NUMBER_OF_ENTRIES_PER_PAGE, keyValueSizeCap, pageSize, Long.SIZE );
        }
    }

    @VisibleForTesting
    public static int keyValueSizeCapFromPageSize( int pageSize )
    {
        return (pageSize - HEADER_LENGTH_DYNAMIC) / LEAST_NUMBER_OF_ENTRIES_PER_PAGE - SIZE_TOTAL_OVERHEAD;
    }

    @Override
    void writeAdditionalHeader( PageCursor cursor )
    {
        setAllocOffset( cursor, pageSize );
        setDeadSpace( cursor, 0 );
    }

    @Override
    KEY keyAt( PageCursor cursor, KEY into, int pos, Type type )
    {
        placeCursorAtActualKey( cursor, pos, type );

        // Read key
        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );
        if ( keyValueSizeTooLarge( keySize, valueSize ) || keySize < 0 )
        {
            readUnreliableKeyValueSize( cursor, keySize, valueSize, keyValueSize, pos );
            return into;
        }
        layout.readKey( cursor, into, keySize );
        return into;
    }

    @Override
    void keyValueAt( PageCursor cursor, KEY intoKey, VALUE intoValue, int pos )
    {
        placeCursorAtActualKey( cursor, pos, LEAF );

        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );
        if ( keyValueSizeTooLarge( keySize, valueSize ) || keySize < 0 || valueSize < 0 )
        {
            readUnreliableKeyValueSize( cursor, keySize, valueSize, keyValueSize, pos );
            return;
        }
        layout.readKey( cursor, intoKey, keySize );
        layout.readValue( cursor, intoValue, valueSize );
    }

    @Override
    void insertKeyAndRightChildAt( PageCursor cursor, KEY key, long child, int pos, int keyCount, long stableGeneration,
            long unstableGeneration )
    {
        // Where to write key?
        int currentKeyOffset = getAllocOffset( cursor );
        int keySize = layout.keySize( key );
        int newKeyOffset = currentKeyOffset - keySize - getOverhead( keySize, 0 );

        // Write key
        cursor.setOffset( newKeyOffset );
        putKeySize( cursor, keySize );
        layout.writeKey( cursor, key );

        // Update alloc space
        setAllocOffset( cursor, newKeyOffset );

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
        int currentKeyValueOffset = getAllocOffset( cursor );
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        int newKeyValueOffset = currentKeyValueOffset - keySize - valueSize - getOverhead( keySize, valueSize );

        // Write key and value
        cursor.setOffset( newKeyValueOffset );
        putKeyValueSize( cursor, keySize, valueSize );
        layout.writeKey( cursor, key );
        layout.writeValue( cursor, value );

        // Update alloc space
        setAllocOffset( cursor, newKeyValueOffset );

        // Write to offset array
        insertSlotsAt( cursor, pos, 1, keyCount, keyPosOffsetLeaf( 0 ), bytesKeyOffset() );
        cursor.setOffset( keyPosOffsetLeaf( pos ) );
        putKeyOffset( cursor, newKeyValueOffset );
    }

    @Override
    void removeKeyValueAt( PageCursor cursor, int pos, int keyCount )
    {
        // Kill actual key
        placeCursorAtActualKey( cursor, pos, LEAF );
        int keyOffset = cursor.getOffset();
        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );
        cursor.setOffset( keyOffset );
        putTombstone( cursor );

        // Update dead space
        int deadSpace = getDeadSpace( cursor );
        setDeadSpace( cursor, deadSpace + keySize + valueSize + getOverhead( keySize, valueSize ) );

        // Remove from offset array
        removeSlotAt( cursor, pos, keyCount, keyPosOffsetLeaf( 0 ), bytesKeyOffset() );
    }

    @Override
    void removeKeyAndRightChildAt( PageCursor cursor, int keyPos, int keyCount )
    {
        // Kill actual key
        placeCursorAtActualKey( cursor, keyPos, INTERNAL );
        int keyOffset = cursor.getOffset();
        int keySize = extractKeySize( readKeyValueSize( cursor ) );
        cursor.setOffset( keyOffset );
        putTombstone( cursor );

        // Update dead space
        int deadSpace = getDeadSpace( cursor );
        setDeadSpace( cursor, deadSpace + keySize + getOverhead( keySize, 0 ) );

        // Remove for offsetArray
        removeSlotAt( cursor, keyPos, keyCount, keyPosOffsetInternal( 0 ), keyChildSize() );

        // Zero pad empty area
        zeroPad( cursor, keyPosOffsetInternal( keyCount - 1 ), bytesKeyOffset() + childSize() );
    }

    @Override
    void removeKeyAndLeftChildAt( PageCursor cursor, int keyPos, int keyCount )
    {
        // Kill actual key
        placeCursorAtActualKey( cursor, keyPos, INTERNAL );
        int keyOffset = cursor.getOffset();
        int keySize = extractKeySize( readKeyValueSize( cursor ) );
        cursor.setOffset( keyOffset );
        putTombstone( cursor );

        // Update dead space
        int deadSpace = getDeadSpace( cursor );
        setDeadSpace( cursor, deadSpace + keySize + getOverhead( keySize, 0 ) );

        // Remove for offsetArray
        removeSlotAt( cursor, keyPos, keyCount, keyPosOffsetInternal( 0 ) - childSize(), keyChildSize() );

        // Move last child
        cursor.copyTo( childOffset( keyCount ), cursor, childOffset( keyCount - 1 ), childSize() );

        // Zero pad empty area
        zeroPad( cursor, keyPosOffsetInternal( keyCount - 1 ), bytesKeyOffset() + childSize() );
    }

    @Override
    boolean setKeyAtInternal( PageCursor cursor, KEY key, int pos )
    {
        placeCursorAtActualKey( cursor, pos, INTERNAL );

        long keyValueSize = readKeyValueSize( cursor );
        int oldKeySize = extractKeySize( keyValueSize );
        int oldValueSize = extractValueSize( keyValueSize );
        if ( keyValueSizeTooLarge( oldKeySize, oldValueSize ) )
        {
            readUnreliableKeyValueSize( cursor, oldKeySize, oldValueSize, keyValueSize, pos );
        }
        int newKeySize = layout.keySize( key );
        if ( newKeySize == oldKeySize )
        {
            // Fine, we can just overwrite
            layout.writeKey( cursor, key );
            return true;
        }
        return false;
    }

    @Override
    VALUE valueAt( PageCursor cursor, VALUE into, int pos )
    {
        placeCursorAtActualKey( cursor, pos, LEAF );

        // Read value
        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );
        if ( keyValueSizeTooLarge( keySize, valueSize ) || keySize < 0 || valueSize < 0 )
        {
            readUnreliableKeyValueSize( cursor, keySize, valueSize, keyValueSize, pos );
            return into;
        }
        progressCursor( cursor, keySize );
        layout.readValue( cursor, into, valueSize );
        return into;
    }

    @Override
    boolean setValueAt( PageCursor cursor, VALUE value, int pos )
    {
        placeCursorAtActualKey( cursor, pos, LEAF );

        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int oldValueSize = extractValueSize( keyValueSize );
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
        cursor.setOffset( childOffset( pos ) );
        writeChild( cursor, child, stableGeneration, unstableGeneration );
    }

    @Override
    public int keyValueSizeCap()
    {
        return keyValueSizeCap;
    }

    @Override
    void validateKeyValueSize( KEY key, VALUE value )
    {
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        if ( keyValueSizeTooLarge( keySize, valueSize ) )
        {
            throw new IllegalArgumentException( "Index key-value size it to large. Please see index documentation for limitations." );
        }
    }

    @Override
    boolean reasonableKeyCount( int keyCount )
    {
        return keyCount >= 0 && keyCount <= totalSpace / SIZE_TOTAL_OVERHEAD;
    }

    @Override
    boolean reasonableChildCount( int childCount )
    {
        return reasonableKeyCount( childCount );
    }

    @Override
    int childOffset( int pos )
    {
        // Child pointer to the left of key at pos
        return keyPosOffsetInternal( pos ) - childSize();
    }

    @Override
    Overflow internalOverflow( PageCursor cursor, int currentKeyCount, KEY newKey )
    {
        // How much space do we have?
        int allocSpace = getAllocSpace( cursor, currentKeyCount, INTERNAL );
        int deadSpace = getDeadSpace( cursor );

        // How much space do we need?
        int neededSpace = totalSpaceOfKeyChild( newKey );

        // There is your answer!
        return neededSpace < allocSpace ? Overflow.NO :
               neededSpace < allocSpace + deadSpace ? Overflow.NO_NEED_DEFRAG : Overflow.YES;
    }

    @Override
    Overflow leafOverflow( PageCursor cursor, int currentKeyCount, KEY newKey, VALUE newValue )
    {
        // How much space do we have?
        int deadSpace = getDeadSpace( cursor );
        int allocSpace = getAllocSpace( cursor, currentKeyCount, LEAF );

        // How much space do we need?
        int neededSpace = totalSpaceOfKeyValue( newKey, newValue );

        // There is your answer!
        return neededSpace < allocSpace ? Overflow.NO :
               neededSpace < allocSpace + deadSpace ? Overflow.NO_NEED_DEFRAG : Overflow.YES;
    }

    @Override
    void defragmentLeaf( PageCursor cursor )
    {
        doDefragment( cursor, LEAF );
    }

    @Override
    void defragmentInternal( PageCursor cursor )
    {
        doDefragment( cursor, INTERNAL );
    }

    private void doDefragment( PageCursor cursor, Type type )
    {
        /*
        The goal is to compact all alive keys in the node
        by reusing the space occupied by dead keys.

        BEFORE
        [8][X][1][3][X][2][X][7][5]

        AFTER
        .........[8][1][3][2][7][5]
            ^ Reclaimed space

        It works like this:
        Work from right to left.
        For each dead space of size X (can be multiple consecutive dead keys)
        Move all neighbouring alive keys to the left of that dead space X bytes to the right.
        Can only move in blocks of size X at the time.

        Step by step:
        [8][X][1][3][X][2][X][7][5]
        [8][X][1][3][X][X][2][7][5]
        [8][X][X][X][1][3][2][7][5]
        [X][X][X][8][1][3][2][7][5]

        Here is how the offsets work
        BEFORE MOVE
                          v       aliveRangeOffset
        [X][_][_][X][_][X][_][_]
                   ^   ^          deadRangeOffset
                   |_____________ moveRangeOffset

        AFTER MOVE
                       v          aliveRangeOffset
        [X][_][_][X][X][_][_][_]
                 ^                 deadRangeOffset
        */

        // Mark all offsets
        deadKeysOffset.clear();
        aliveKeysOffset.clear();
        if ( type == INTERNAL )
        {
            recordDeadAndAliveInternal( cursor, deadKeysOffset, aliveKeysOffset );
        }
        else
        {
            recordDeadAndAliveLeaf( cursor, deadKeysOffset, aliveKeysOffset );
        }

        // Cursors into field byte arrays
        int oldOffsetCursor = 0;
        int newOffsetCursor = 0;

        int aliveRangeOffset = pageSize; // Everything after this point is alive
        int deadRangeOffset; // Everything between this point and aliveRangeOffset is dead space

        // Rightmost alive keys does not need to move
        while ( deadKeysOffset.peek() < aliveKeysOffset.peek() )
        {
            aliveRangeOffset = aliveKeysOffset.poll();
        }

        do
        {
            // Locate next range of dead keys
            deadRangeOffset = aliveRangeOffset;
            while ( aliveKeysOffset.peek() < deadKeysOffset.peek() )
            {
                deadRangeOffset = deadKeysOffset.poll();
            }

            // Locate next range of alive keys
            int moveOffset = deadRangeOffset;
            while ( deadKeysOffset.peek() < aliveKeysOffset.peek() )
            {
                int moveKey = aliveKeysOffset.poll();
                oldOffset[oldOffsetCursor++] = moveKey;
                moveOffset = moveKey;
            }

            // Update offset mapping
            int deadRangeSize = aliveRangeOffset - deadRangeOffset;
            while ( oldOffsetCursor > newOffsetCursor )
            {
                newOffset[newOffsetCursor] = oldOffset[newOffsetCursor] + deadRangeSize;
                newOffsetCursor++;
            }

            // Do move
            while ( moveOffset < (deadRangeOffset - deadRangeSize) )
            {
                // Move one block
                deadRangeOffset -= deadRangeSize;
                aliveRangeOffset -= deadRangeSize;
                cursor.copyTo( deadRangeOffset, cursor, aliveRangeOffset, deadRangeSize );
            }
            // Move the last piece
            int lastBlockSize = deadRangeOffset - moveOffset;
            if ( lastBlockSize > 0 )
            {
                deadRangeOffset -= lastBlockSize;
                aliveRangeOffset -= lastBlockSize;
                cursor.copyTo( deadRangeOffset, cursor, aliveRangeOffset, lastBlockSize );
            }
        }
        while ( !aliveKeysOffset.isEmpty() );
        // Update allocOffset
        int prevAllocOffset = getAllocOffset( cursor );
        setAllocOffset( cursor, aliveRangeOffset );

        // Zero pad reclaimed area
        zeroPad( cursor, prevAllocOffset, aliveRangeOffset - prevAllocOffset );

        // Update offset array
        int keyCount = keyCount( cursor );
        keyPos:
        for ( int pos = 0; pos < keyCount; pos++ )
        {
            int keyPosOffset = keyPosOffset( pos, type );
            cursor.setOffset( keyPosOffset );
            int keyOffset = readKeyOffset( cursor );
            for ( int index = 0; index < oldOffsetCursor; index++ )
            {
                if ( keyOffset == oldOffset[index] )
                {
                    // Overwrite with new offset
                    cursor.setOffset( keyPosOffset );
                    putKeyOffset( cursor, newOffset[index] );
                    continue keyPos;
                }
            }
        }

        // Update dead space
        setDeadSpace( cursor, 0 );
    }

    @Override
    boolean leafUnderflow( PageCursor cursor, int keyCount )
    {
        int halfSpace = this.halfSpace;
        int allocSpace = getAllocSpace( cursor, keyCount, LEAF );
        int deadSpace = getDeadSpace( cursor );
        int availableSpace = allocSpace + deadSpace;

        return availableSpace > halfSpace;
    }

    @Override
    int canRebalanceLeaves( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount )
    {
        int leftActiveSpace = totalActiveSpace( leftCursor, leftKeyCount );
        int rightActiveSpace = totalActiveSpace( rightCursor, rightKeyCount );

        if ( leftActiveSpace + rightActiveSpace < totalSpace )
        {
            // We can merge
            return -1;
        }
        if ( leftActiveSpace < rightActiveSpace )
        {
            // Moving keys to the right will only create more imbalance
            return 0;
        }

        int prevDelta;
        int currentDelta = Math.abs( leftActiveSpace - rightActiveSpace );
        int keysToMove = 0;
        int lastChunkSize;
        do
        {
            keysToMove++;
            lastChunkSize = totalSpaceOfKeyValue( leftCursor, leftKeyCount - keysToMove );
            leftActiveSpace -= lastChunkSize;
            rightActiveSpace += lastChunkSize;

            prevDelta = currentDelta;
            currentDelta = Math.abs( leftActiveSpace - rightActiveSpace );
        }
        while ( currentDelta < prevDelta );
        keysToMove--; // Move back to optimal split
        leftActiveSpace += lastChunkSize;
        rightActiveSpace -= lastChunkSize;

        int halfSpace = this.halfSpace;
        boolean canRebalance = leftActiveSpace > halfSpace && rightActiveSpace > halfSpace;
        return canRebalance ? keysToMove : 0;
    }

    @Override
    boolean canMergeLeaves( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount )
    {
        int leftActiveSpace = totalActiveSpace( leftCursor, leftKeyCount );
        int rightActiveSpace = totalActiveSpace( rightCursor, rightKeyCount );
        int totalSpace = this.totalSpace;
        return totalSpace >= leftActiveSpace + rightActiveSpace;
    }

    @Override
    void doSplitLeaf( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int insertPos, KEY newKey,
            VALUE newValue, KEY newSplitter )
    {
        // Find middle
        int keyCountAfterInsert = leftKeyCount + 1;
        int middlePos = middlePosInLeaf( leftCursor, insertPos, newKey, newValue, keyCountAfterInsert );

        if ( middlePos == insertPos )
        {
            layout.copyKey( newKey, newSplitter );
        }
        else
        {
            keyAt( leftCursor, newSplitter, insertPos < middlePos ? middlePos - 1 : middlePos, LEAF );
        }
        int rightKeyCount = keyCountAfterInsert - middlePos;

        if ( insertPos < middlePos )
        {
            //                  v-------v       copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,X,_,_,_,_,_,_,_
            // middle           ^
            moveKeysAndValues( leftCursor, middlePos - 1, rightCursor, 0, rightKeyCount );
            defragmentLeaf( leftCursor );
            insertKeyValueAt( leftCursor, newKey, newValue, insertPos, middlePos - 1 );
        }
        else
        {
            //                  v---v           first copy
            //                        v-v       second copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,_,_,_,_,_,X,_,_
            // middle           ^

            // Copy everything in one go
            int newInsertPos = insertPos - middlePos;
            int keysToMove = leftKeyCount - middlePos;
            moveKeysAndValues( leftCursor, middlePos, rightCursor, 0, keysToMove );
            defragmentLeaf( leftCursor );
            insertKeyValueAt( rightCursor, newKey, newValue, newInsertPos, keysToMove );
        }
        TreeNode.setKeyCount( leftCursor, middlePos );
        TreeNode.setKeyCount( rightCursor, rightKeyCount );
    }

    @Override
    void doSplitInternal( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int insertPos, KEY newKey,
            long newRightChild, long stableGeneration, long unstableGeneration, KEY newSplitter )
    {
        int keyCountAfterInsert = leftKeyCount + 1;
        int middlePos = middleInternal( leftCursor, insertPos, newKey, keyCountAfterInsert );

        if ( middlePos == insertPos )
        {
            layout.copyKey( newKey, newSplitter );
        }
        else
        {
            keyAt( leftCursor, newSplitter, insertPos < middlePos ? middlePos - 1 : middlePos, INTERNAL );
        }
        int rightKeyCount = keyCountAfterInsert - middlePos - 1; // -1 because don't keep prim key in internal

        if ( insertPos < middlePos )
        {
            //                         v-------v       copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,X,_,_,_,_,_,_,_,_
            // insert child -,-,-,x,-,-,-,-,-,-,-,-
            // middle key              ^

            moveKeysAndChildren( leftCursor, middlePos, rightCursor, 0, rightKeyCount, true );
            // Rightmost key in left is the one we send up to parent, remove it from here.
            removeKeyAndRightChildAt( leftCursor, middlePos - 1, middlePos );
            defragmentInternal( leftCursor );
            insertKeyAndRightChildAt( leftCursor, newKey, newRightChild, insertPos, middlePos - 1, stableGeneration, unstableGeneration );
        }
        else
        {
            // pos > middlePos
            //                         v-v          first copy
            //                             v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,_,_,X,_,_,_
            // insert child -,-,-,-,-,-,-,-,x,-,-,-
            // middle key              ^

            // pos == middlePos
            //                                      first copy
            //                         v-v-v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,X,_,_,_,_,_
            // insert child -,-,-,-,-,-,x,-,-,-,-,-
            // middle key              ^

            // Keys
            if ( insertPos == middlePos )
            {
                int copyFrom = middlePos;
                int copyCount = leftKeyCount - copyFrom;
                moveKeysAndChildren( leftCursor, copyFrom, rightCursor, 0, copyCount, false );
                defragmentInternal( leftCursor );
                setChildAt( rightCursor, newRightChild, 0, stableGeneration, unstableGeneration );
            }
            else
            {
                int copyFrom = middlePos + 1;
                int copyCount = leftKeyCount - copyFrom;
                moveKeysAndChildren( leftCursor, copyFrom, rightCursor, 0, copyCount, true );
                // Rightmost key in left is the one we send up to parent, remove it from here.
                removeKeyAndRightChildAt( leftCursor, middlePos, middlePos + 1 );
                defragmentInternal( leftCursor );
                insertKeyAndRightChildAt( rightCursor, newKey, newRightChild, insertPos - copyFrom, copyCount,
                        stableGeneration, unstableGeneration );
            }
        }
        TreeNode.setKeyCount( leftCursor, middlePos );
        TreeNode.setKeyCount( rightCursor, rightKeyCount );
    }

    @Override
    void moveKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount,
            int fromPosInLeftNode )
    {
        defragmentLeaf( rightCursor );
        int numberOfKeysToMove = leftKeyCount - fromPosInLeftNode;

        // Push keys and values in right sibling to the right
        insertSlotsAt( rightCursor, 0, numberOfKeysToMove, rightKeyCount, keyPosOffsetLeaf( 0 ), bytesKeyOffset() );

        // Move (also updates keyCount of left)
        moveKeysAndValues( leftCursor, fromPosInLeftNode, rightCursor, 0, numberOfKeysToMove );

        // Right keyCount
        setKeyCount( rightCursor, rightKeyCount + numberOfKeysToMove );
    }

    // NOTE: Does update keyCount
    private void moveKeysAndValues( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count )
    {
        int firstAllocOffset = getAllocOffset( toCursor );
        int toAllocOffset = firstAllocOffset;
        for ( int i = 0; i < count; i++, toPos++ )
        {
            toAllocOffset = moveRawKeyValue( fromCursor, fromPos + i, toCursor, toAllocOffset );
            toCursor.setOffset( keyPosOffsetLeaf( toPos ) );
            putKeyOffset( toCursor, toAllocOffset );
        }
        setAllocOffset( toCursor, toAllocOffset );

        // Update deadspace
        int deadSpace = getDeadSpace( fromCursor );
        int totalMovedBytes = firstAllocOffset - toAllocOffset;
        setDeadSpace( fromCursor, deadSpace + totalMovedBytes );

        // Key count
        setKeyCount( fromCursor, fromPos );
    }

    /**
     * Transfer key and value from logical position in 'from' to physical position next to current alloc offset in 'to'.
     * Mark transferred key as dead.
     * @return new alloc offset in 'to'
     */
    private int moveRawKeyValue( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toAllocOffset )
    {
        // What to copy?
        placeCursorAtActualKey( fromCursor, fromPos, LEAF );
        int fromKeyOffset = fromCursor.getOffset();
        long keyValueSize = readKeyValueSize( fromCursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );

        // Copy
        int toCopy = getOverhead( keySize, valueSize ) + keySize + valueSize;
        int newRightAllocSpace = toAllocOffset - toCopy;
        fromCursor.copyTo( fromKeyOffset, toCursor, newRightAllocSpace, toCopy );

        // Put tombstone
        fromCursor.setOffset( fromKeyOffset );
        putTombstone( fromCursor );
        return newRightAllocSpace;
    }

    @Override
    void copyKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount )
    {
        defragmentLeaf( rightCursor );

        // Push keys and values in right sibling to the right
        insertSlotsAt( rightCursor, 0, leftKeyCount, rightKeyCount, keyPosOffsetLeaf( 0 ), bytesKeyOffset() );

        // Copy
        copyKeysAndValues( leftCursor, 0, rightCursor, 0, leftKeyCount );

        // KeyCount
        setKeyCount( rightCursor, rightKeyCount + leftKeyCount );
    }

    private void copyKeysAndValues( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count )
    {
        int toAllocOffset = getAllocOffset( toCursor );
        for ( int i = 0; i < count; i++, toPos++ )
        {
            toAllocOffset = copyRawKeyValue( fromCursor, fromPos + i, toCursor, toAllocOffset );
            toCursor.setOffset( keyPosOffsetLeaf( toPos ) );
            putKeyOffset( toCursor, toAllocOffset );
        }
        setAllocOffset( toCursor, toAllocOffset );
    }

    /**
     * Copy key and value from logical position in 'from' tp physical position next to current alloc offset in 'to'.
     * Does NOT mark transferred key as dead.
     * @return new alloc offset in 'to'
     */
    private int copyRawKeyValue( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toAllocOffset )
    {
        // What to copy?
        placeCursorAtActualKey( fromCursor, fromPos, LEAF );
        int fromKeyOffset = fromCursor.getOffset();
        long keyValueSize = readKeyValueSize( fromCursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );

        // Copy
        int toCopy = getOverhead( keySize, valueSize ) + keySize + valueSize;
        int newRightAllocSpace = toAllocOffset - toCopy;
        fromCursor.copyTo( fromKeyOffset, toCursor, newRightAllocSpace, toCopy );
        return newRightAllocSpace;
    }

    private int getAllocSpace( PageCursor cursor, int keyCount, Type type )
    {
        int allocOffset = getAllocOffset( cursor );
        int endOfOffsetArray = type == LEAF ? keyPosOffsetLeaf( keyCount ) : keyPosOffsetInternal( keyCount );
        return allocOffset - endOfOffsetArray;
    }

    private void recordDeadAndAliveLeaf( PageCursor cursor, PrimitiveIntStack deadKeysOffset, PrimitiveIntStack aliveKeysOffset )
    {
        int currentOffset = getAllocOffset( cursor );
        while ( currentOffset < pageSize )
        {
            cursor.setOffset( currentOffset );
            long keyValueSize = readKeyValueSize( cursor );
            int keySize = extractKeySize( keyValueSize );
            int valueSize = extractValueSize( keyValueSize );
            boolean dead = extractTombstone( keyValueSize );

            if ( dead )
            {
                deadKeysOffset.push( currentOffset );
            }
            else
            {
                aliveKeysOffset.push( currentOffset );
            }
            currentOffset += keySize + valueSize + getOverhead( keySize, valueSize );
        }
    }

    private void recordDeadAndAliveInternal( PageCursor cursor, PrimitiveIntStack deadKeysOffset, PrimitiveIntStack aliveKeysOffset )
    {
        int currentOffset = getAllocOffset( cursor );
        while ( currentOffset < pageSize )
        {
            cursor.setOffset( currentOffset );
            long keyValueSize = readKeyValueSize( cursor );
            int keySize = extractKeySize( keyValueSize );
            boolean dead = extractTombstone( keyValueSize );

            if ( dead )
            {
                deadKeysOffset.push( currentOffset );
            }
            else
            {
                aliveKeysOffset.push( currentOffset );
            }
            currentOffset += keySize + getOverhead( keySize, 0 );
        }
    }

    // NOTE: Does NOT update keyCount
    private void moveKeysAndChildren( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count,
            boolean includeLeftMostChild )
    {
        if ( count == 0 && !includeLeftMostChild )
        {
            // Nothing to move
            return;
        }

        // All children
        // This will also copy key offsets but those will be overwritten below.
        int childFromOffset = includeLeftMostChild ? childOffset( fromPos ) : childOffset( fromPos + 1 );
        int childToOffset = childOffset( fromPos + count ) + childSize();
        int lengthInBytes = childToOffset - childFromOffset;
        int targetOffset = includeLeftMostChild ? childOffset( 0 ) : childOffset( 1 );
        fromCursor.copyTo( childFromOffset, toCursor, targetOffset, lengthInBytes );

        // Move actual keys and update pointers
        int toAllocOffset = getAllocOffset( toCursor );
        int firstAllocOffset = toAllocOffset;
        for ( int i = 0; i < count; i++, toPos++ )
        {
            // Key
            toAllocOffset = transferRawKey( fromCursor, fromPos + i, toCursor, toAllocOffset );
            toCursor.setOffset( keyPosOffsetInternal( toPos ) );
            putKeyOffset( toCursor, toAllocOffset );
        }
        setAllocOffset( toCursor, toAllocOffset );

        // Update deadspace
        int deadSpace = getDeadSpace( fromCursor );
        int totalMovedBytes = firstAllocOffset - toAllocOffset;
        setDeadSpace( fromCursor, deadSpace + totalMovedBytes );

        // Zero pad empty area
        zeroPad( fromCursor, childFromOffset, lengthInBytes );
    }

    private void zeroPad( PageCursor fromCursor, int fromOffset, int lengthInBytes )
    {
        fromCursor.setOffset( fromOffset );
        fromCursor.putBytes( lengthInBytes, (byte) 0 );
    }

    private int transferRawKey( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toAllocOffset )
    {
        // What to copy?
        placeCursorAtActualKey( fromCursor, fromPos, INTERNAL );
        int fromKeyOffset = fromCursor.getOffset();
        int keySize = extractKeySize( readKeyValueSize( fromCursor ) );

        // Copy
        int toCopy = getOverhead( keySize, 0 ) + keySize;
        toAllocOffset -= toCopy;
        fromCursor.copyTo( fromKeyOffset, toCursor, toAllocOffset, toCopy );

        // Put tombstone
        fromCursor.setOffset( fromKeyOffset );
        putTombstone( fromCursor );
        return toAllocOffset;
    }

    private int middleInternal( PageCursor cursor, int insertPos, KEY newKey, int keyCountAfterInsert )
    {
        int halfSpace = this.halfSpace;
        int middle = 0;
        int currentPos = 0;
        int middleSpace = childSize(); // Leftmost child will always be included in left side
        int currentDelta = Math.abs( middleSpace - halfSpace );
        int prevDelta;
        boolean includedNew = false;

        do
        {
            // We may come closer to split by keeping one more in left
            int space;
            if ( currentPos == insertPos & !includedNew )
            {
                space = totalSpaceOfKeyChild( newKey );
                includedNew = true;
                currentPos--;
            }
            else
            {
                space = totalSpaceOfKeyChild( cursor, currentPos );
            }
            middleSpace += space;
            prevDelta = currentDelta;
            currentDelta = Math.abs( middleSpace - halfSpace );
            middle++;
            currentPos++;
        }
        while ( currentDelta < prevDelta && currentPos < keyCountAfterInsert );
        middle--; // Step back to the pos that most equally divide the available space in two
        return middle;
    }

    /**
     * Calculates a valid and as optimal as possible position where to split a leaf if inserting a key overflows.
     * There are a couple of goals/conditions which drives the search for it:
     * <ul>
     *     <li>The returned position will be one where the keys ending up in the left and right leaves respectively are guaranteed to fit.</li>
     *     <li>Out of those possible positions the one which is closest to the "halfSpace" of a leaf will be selected</li>
     * </ul>
     *
     * @param cursor {@link PageCursor} to use for reading sizes of existing entries.
     * @param insertPos the pos which the new key will be inserted at.
     * @param newKey key to be inserted.
     * @param newValue value to be inserted.
     * @param keyCountAfterInsert key count including the new key.
     * @return the pos where to split.
     */
    private int middlePosInLeaf( PageCursor cursor, int insertPos, KEY newKey, VALUE newValue, int keyCountAfterInsert )
    {
        int halfSpace = this.halfSpace;
        int middle = 0;
        int currentPos = 0;
        int accumulatedSpace = 0;
        int currentDelta = halfSpace;
        int prevDelta;
        int spaceOfNewKey = totalSpaceOfKeyValue( newKey, newValue );
        int totalSpaceIncludingNewKey = totalActiveSpace( cursor, keyCountAfterInsert - 1 ) + spaceOfNewKey;
        boolean includedNew = false;
        boolean prevPosPossible;

        if ( totalSpaceIncludingNewKey > totalSpace * 2 )
        {
            throw new IllegalStateException(
                    format( "There's not enough space to insert new key, even when splitting the leaf. Space needed:%d, max space allowed:%d",
                            totalSpaceIncludingNewKey, totalSpace * 2 ) );
        }

        do
        {
            prevPosPossible = totalSpaceIncludingNewKey - accumulatedSpace <= totalSpace;

            // We may come closer to split by keeping one more in left
            int currentSpace;
            if ( currentPos == insertPos & !includedNew )
            {
                currentSpace = spaceOfNewKey;
                includedNew = true;
                currentPos--;
            }
            else
            {
                currentSpace = totalSpaceOfKeyValue( cursor, currentPos );
            }
            accumulatedSpace += currentSpace;
            prevDelta = currentDelta;
            currentDelta = Math.abs( accumulatedSpace - halfSpace );
            currentPos++;
            middle++;
        }
        while ( currentDelta < prevDelta && currentPos < keyCountAfterInsert );
        // If previous position is possible then step back one pos since it divides the space most equally
        if ( prevPosPossible )
        {
            middle--;
        }
        return middle;
    }

    private int totalActiveSpace( PageCursor cursor, int keyCount )
    {
        int deadSpace = getDeadSpace( cursor );
        int allocSpace = getAllocSpace( cursor, keyCount, LEAF );
        return totalSpace - deadSpace - allocSpace;
    }

    private int totalSpaceOfKeyValue( KEY key, VALUE value )
    {
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        return bytesKeyOffset() + getOverhead( keySize, valueSize ) + keySize + valueSize;
    }

    private int totalSpaceOfKeyChild( KEY key )
    {
        int keySize = layout.keySize( key );
        return bytesKeyOffset() + getOverhead( keySize, 0 ) + childSize() + keySize;
    }

    private int totalSpaceOfKeyValue( PageCursor cursor, int pos )
    {
        placeCursorAtActualKey( cursor, pos, LEAF );
        long keyValueSize = readKeyValueSize( cursor );
        int keySize = extractKeySize( keyValueSize );
        int valueSize = extractValueSize( keyValueSize );
        return bytesKeyOffset() + getOverhead( keySize, valueSize ) + keySize + valueSize;
    }

    private int totalSpaceOfKeyChild( PageCursor cursor, int pos )
    {
        placeCursorAtActualKey( cursor, pos, INTERNAL );
        int keySize = extractKeySize( readKeyValueSize( cursor ) );
        return bytesKeyOffset() + getOverhead( keySize, 0 ) + childSize() + keySize;
    }

    private void setAllocOffset( PageCursor cursor, int allocOffset )
    {
        PageCursorUtil.putUnsignedShort( cursor, BYTE_POS_ALLOCOFFSET, allocOffset );
    }

    int getAllocOffset( PageCursor cursor )
    {
        return PageCursorUtil.getUnsignedShort( cursor, BYTE_POS_ALLOCOFFSET );
    }

    private void setDeadSpace( PageCursor cursor, int deadSpace )
    {
        putUnsignedShort( cursor, BYTE_POS_DEADSPACE, deadSpace );
    }

    private int getDeadSpace( PageCursor cursor )
    {
        return PageCursorUtil.getUnsignedShort( cursor, BYTE_POS_DEADSPACE );
    }

    private void placeCursorAtActualKey( PageCursor cursor, int pos, Type type )
    {
        // Set cursor to correct place in offset array
        int keyPosOffset = keyPosOffset( pos, type );
        cursor.setOffset( keyPosOffset );

        // Read actual offset to key
        int keyOffset = readKeyOffset( cursor );

        // Verify offset is reasonable
        if ( keyOffset >= pageSize || keyOffset < HEADER_LENGTH_DYNAMIC )
        {
            cursor.setCursorException( format( "Tried to read key on offset=%d, headerLength=%d, pageSize=%d, pos=%d",
                    keyOffset, HEADER_LENGTH_DYNAMIC, pageSize, pos ) );
            return;
        }

        // Set cursor to actual offset
        cursor.setOffset( keyOffset );
    }

    private void readUnreliableKeyValueSize( PageCursor cursor, int keySize, int valueSize, long keyValueSize, int pos )
    {
        cursor.setCursorException( format( "Read unreliable key, keySize=%d, valueSize=%d, keyValueSizeCap=%d, keyHasTombstone=%b, pos=%d",
                keySize, valueSize, keyValueSizeCap(), extractTombstone( keyValueSize ), pos ) );
    }

    private boolean keyValueSizeTooLarge( int keySize, int valueSize )
    {
        return keySize + valueSize > keyValueSizeCap();
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
        return HEADER_LENGTH_DYNAMIC + pos * bytesKeyOffset();
    }

    private int keyPosOffsetInternal( int pos )
    {
        // header + childPointer + pos * (keyPosOffsetSize + childPointer)
        return HEADER_LENGTH_DYNAMIC + childSize() + pos * keyChildSize();
    }

    private int keyChildSize()
    {
        return bytesKeyOffset() + SIZE_PAGE_REFERENCE;
    }

    private int childSize()
    {
        return SIZE_PAGE_REFERENCE;
    }

    private static int bytesKeyOffset()
    {
        return SIZE_OFFSET;
    }

    private static int bytesPageOffset()
    {
        return SIZE_OFFSET;
    }

    @Override
    public String toString()
    {
        return "TreeNodeDynamicSize[pageSize:" + pageSize + ", keyValueSizeCap:" + keyValueSizeCap() + "]";
    }

    private String asString( PageCursor cursor, boolean includeValue, boolean includeAllocSpace,
            long stableGeneration, long unstableGeneration )
    {
        int currentOffset = cursor.getOffset();
        // [header] <- dont care
        // LEAF:     [allocOffset=][child0,key0*,child1,...][keySize|key][keySize|key]
        // INTERNAL: [allocOffset=][key0*,key1*,...][offset|keySize|valueSize|key][keySize|valueSize|key]

        Type type = isInternal( cursor ) ? INTERNAL : LEAF;

        // HEADER
        int allocOffset = getAllocOffset( cursor );
        int deadSpace = getDeadSpace( cursor );
        String additionalHeader = "{" + cursor.getCurrentPageId() + "} [allocOffset=" + allocOffset + " deadSpace=" + deadSpace + "] ";

        // OFFSET ARRAY
        String offsetArray = readOffsetArray( cursor, stableGeneration, unstableGeneration, type );

        // ALLOC SPACE
        String allocSpace = "";
        if ( includeAllocSpace )
        {
            allocSpace = readAllocSpace( cursor, allocOffset, type );
        }

        // KEYS
        KEY readKey = layout.newKey();
        VALUE readValue = layout.newValue();
        StringJoiner keys = new StringJoiner( " " );
        cursor.setOffset( allocOffset );
        while ( cursor.getOffset() < cursor.getCurrentPageSize() )
        {
            StringJoiner singleKey = new StringJoiner( "|" );
            singleKey.add( Integer.toString( cursor.getOffset() ) );
            long keyValueSize = readKeyValueSize( cursor );
            int keySize = extractKeySize( keyValueSize );
            int valueSize = 0;
            if ( type == LEAF )
            {
                valueSize = extractValueSize( keyValueSize );
            }
            if ( DynamicSizeUtil.extractTombstone( keyValueSize ) )
            {
                singleKey.add( "X" );
            }
            else
            {
                singleKey.add( "_" );
            }
            layout.readKey( cursor, readKey, keySize );
            if ( type == LEAF )
            {
                layout.readValue( cursor, readValue, valueSize );
            }
            singleKey.add( Integer.toString( keySize ) );
            if ( type == LEAF && includeValue )
            {
                singleKey.add( Integer.toString( valueSize ) );
            }
            singleKey.add( readKey.toString() );
            if ( type == LEAF && includeValue )
            {
                singleKey.add( readValue.toString() );
            }
            keys.add( singleKey.toString() );
        }

        cursor.setOffset( currentOffset );
        return additionalHeader + offsetArray + " " + allocSpace + " " + keys;
    }

    @SuppressWarnings( "unused" )
    @Override
    void printNode( PageCursor cursor, boolean includeValue, boolean includeAllocSpace, long stableGeneration, long unstableGeneration )
    {
        System.out.println( asString( cursor, includeValue, includeAllocSpace, stableGeneration, unstableGeneration ) );
    }

    private String readAllocSpace( PageCursor cursor, int allocOffset, Type type )
    {
        int keyCount = keyCount( cursor );
        int endOfOffsetArray = type == INTERNAL ? keyPosOffsetInternal( keyCount ) : keyPosOffsetLeaf( keyCount );
        cursor.setOffset( endOfOffsetArray );
        int bytesToRead = allocOffset - endOfOffsetArray;
        byte[] allocSpace = new byte[bytesToRead];
        cursor.getBytes( allocSpace );
        for ( byte b : allocSpace )
        {
            if ( b != 0 )
            {
                return "v" + endOfOffsetArray + ">" + bytesToRead + "|" + Arrays.toString( allocSpace );
            }
        }
        return "v" + endOfOffsetArray + ">" + bytesToRead + "|[0...]";
    }

    private String readOffsetArray( PageCursor cursor, long stableGeneration, long unstableGeneration, Type type )
    {
        int keyCount = keyCount( cursor );
        StringJoiner offsetArray = new StringJoiner( " " );
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
