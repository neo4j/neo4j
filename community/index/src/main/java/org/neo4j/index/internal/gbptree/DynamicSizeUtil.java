/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.BASE_HEADER_LENGTH;
import static org.neo4j.index.internal.gbptree.TreeNodeUtil.SIZE_PAGE_REFERENCE;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.pagecache.PageCursorUtil.getUnsignedShort;
import static org.neo4j.io.pagecache.PageCursorUtil.putUnsignedShort;

import org.eclipse.collections.api.block.function.primitive.IntToIntFunction;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.util.VisibleForTesting;

/**
 * Gather utility methods for reading and writing individual dynamic sized
 * keys. It thus define the layout for:
 * - Key pointer in offset array (K*), 2B
 * - keyValueSize, 1B-4B
 * - Key tombstone, first bit in keyValueSize
 *
 * Format of key/value size is dynamic in itself, first byte being:
 * <pre>
 * [T,K,V,k,k,k,k,k]
 * </pre>
 * If {@code T} is set key is dead.
 * If {@code K} is set the next byte contains the higher order bits of the key size.
 * If {@code V} is set there is a value size to be read directly after key size.
 * This first byte can fit key size <= 31 (0x1F) and we only need the second byte if key size is larger.
 *
 * The second key byte is:
 * <pre>
 * [O,k,k,k,k,k,k,k]
 * </pre>
 * If {@code O} is set there is either an offload id to be read after keyValueSize.
 * Together with the second byte we can fit key size <= 4095 (0xFFF).
 *
 * Byte following key size bytes (second or third byte depending on how many bytes needed for key size):
 * <pre>
 * [V,v,v,v,v,v,v,v]
 * </pre>
 * If {@code V} is set the next byte contains the higher order bits of the value size.
 * This first value size byte can fit value size <= 127 (0x7F) and with the second byte we can fit value size <= 32767 (0x7FFF).
 *
 * So in total key/value size has seven different looks (not including tombstone being set or not set):
 * <pre>
 * One byte key, no value
 * [0,0,0,k,k,k,k,k]
 *
 * One byte key, one byte value
 * [0,0,1,k,k,k,k,k][0,v,v,v,v,v,v,v]
 *
 * One byte key, two byte value
 * [0,0,1,k,k,k,k,k][1,v,v,v,v,v,v,v][v,v,v,v,v,v,v,v]
 *
 * Two byte key, no value
 * [0,1,0,k,k,k,k,k][0,k,k,k,k,k,k,k]
 *
 * Two byte key, one byte value
 * [0,1,1,k,k,k,k,k][0,k,k,k,k,k,k,k][0,v,v,v,v,v,v,v]
 *
 * Two byte key, two byte value
 * [0,1,1,k,k,k,k,k][0,k,k,k,k,k,k,k][1,v,v,v,v,v,v,v][v,v,v,v,v,v,v,v]
 *
 * Offload entry
 * [0,1,0,_,_,_,_,_][1,_,_,_,_,_,_,_][offloadId 8B]
 *
 * </pre>
 * This key/value size format is used, both for leaves and internal nodes even though internal nodes can never have values.
 *
 * The most significant key bit in the second byte (shown as 0) is not needed for the discrete key sizes for our 8k page size
 * and is to be considered reserved for future use.
 *
 * Relative layout of key and key_value
 * KeyOffset points to the exact offset where key entry or key_value entry
 * can be read.
 * key entry - [keyValueSize 1B-2B|actualKey]
 * key_value entry - [keyValueSize 1B-4B|actualKey|actualValue]
 *
 * Tombstone
 * First bit in keyValueSize is used as a tombstone, set to 1 if key is dead.
 */
public class DynamicSizeUtil {
    public static final int OFFSET_SIZE = 2;
    public static final int KEY_OFFSET_AND_CHILD_SIZE = OFFSET_SIZE + SIZE_PAGE_REFERENCE;
    public static final int BYTE_POS_ALLOC_OFFSET = BASE_HEADER_LENGTH;
    public static final int BYTE_POS_DEAD_SPACE = BYTE_POS_ALLOC_OFFSET + OFFSET_SIZE;
    public static final int HEADER_LENGTH_DYNAMIC = BYTE_POS_DEAD_SPACE + OFFSET_SIZE;

    static final int MIN_SIZE_KEY_VALUE_SIZE = 1 /*key*/ /*0B value*/;
    static final int MAX_SIZE_KEY_VALUE_SIZE = 2 /*key*/ + 2 /*value*/;
    static final int SIZE_OFFLOAD_ID = Long.BYTES;
    /**
     * Offsets less than this can be encoded with 2 bytes
     */
    @VisibleForTesting
    static final int SUPPORTED_PAGE_SIZE_LIMIT = (int) kibiBytes(64);

    private static final int FLAG_FIRST_BYTE_TOMBSTONE = 0x80;
    private static final int FLAG_SECOND_BYTE_OFFLOAD = 0x80;
    private static final long FLAG_READ_TOMBSTONE = 0x80000000_00000000L;
    private static final long FLAG_READ_OFFLOAD = 0x40000000_00000000L;
    // mask for one-byte key size to map to the k's in [_,_,_,k,k,k,k,k]
    static final int MASK_ONE_BYTE_KEY_SIZE = 0x1F;
    // max two-byte key size to map to the k's in [_,_,_,k,k,k,k,k][_,k,k,k,k,k,k,k]
    static final int MAX_TWO_BYTE_KEY_SIZE = 0xFFF;
    // mask for one-byte value size to map to the v's in [_,v,v,v,v,v,v,v]
    static final int MASK_ONE_BYTE_VALUE_SIZE = 0x7F;
    // max two-byte value size to map to the v's in [_,v,v,v,v,v,v,v][v,v,v,v,v,v,v,v]
    static final int MAX_TWO_BYTE_VALUE_SIZE = 0x7FFF;
    private static final int FLAG_HAS_VALUE_SIZE = 0x20;
    private static final int FLAG_ADDITIONAL_KEY_SIZE = 0x40;
    private static final int FLAG_ADDITIONAL_VALUE_SIZE = 0x80;
    private static final int SHIFT_LSB_KEY_SIZE = 5;
    private static final int SHIFT_LSB_VALUE_SIZE = 7;
    /**
     * This is the fixed key value size cap in 4.0 and it is based on
     * {@link OffloadStoreImpl#keyValueSizeCapFromPageSize(int)} with pageSize=8192.
     * In 4.2 the possibility to have larger page cache pages was introduced,
     * but we still want to keep the same key value size cap for simplicity.
     */
    private static final int FIXED_MAX_KEY_VALUE_SIZE_CAP = 8175;

    static final int LEAST_NUMBER_OF_ENTRIES_PER_PAGE = 2;
    private static final int MINIMUM_ENTRY_SIZE_CAP = Long.BYTES;

    /**
     * Put key value size for inlined entries only, supports key size up to 4095
     */
    public static void putKeyValueSize(PageCursor cursor, int keySize, int valueSize) {
        boolean hasAdditionalKeySize = keySize > MASK_ONE_BYTE_KEY_SIZE;
        boolean hasValueSize = valueSize > 0;

        // Key size
        {
            byte firstByte = (byte) (keySize & MASK_ONE_BYTE_KEY_SIZE); // Least significant 5 bits
            if (hasAdditionalKeySize) {
                firstByte |= FLAG_ADDITIONAL_KEY_SIZE;
                if (keySize > MAX_TWO_BYTE_KEY_SIZE) {
                    throw new IllegalArgumentException(format(
                            "Max supported inline key size is %d, but tried to store key of size %d.",
                            MAX_TWO_BYTE_KEY_SIZE, keySize));
                }
            }
            if (hasValueSize) {
                firstByte |= FLAG_HAS_VALUE_SIZE;
            }
            cursor.putByte(firstByte);

            if (hasAdditionalKeySize) {
                // Assuming no key size larger than maxKeySize limit
                cursor.putByte((byte) (keySize >> SHIFT_LSB_KEY_SIZE));
            }
        }

        // Value size
        {
            if (hasValueSize) {
                boolean needsAdditionalValueSize = valueSize > MASK_ONE_BYTE_VALUE_SIZE;
                byte firstByte = (byte) (valueSize & MASK_ONE_BYTE_VALUE_SIZE); // Least significant 7 bits
                if (needsAdditionalValueSize) {
                    if (valueSize > MAX_TWO_BYTE_VALUE_SIZE) {
                        throw new IllegalArgumentException(format(
                                "Max supported value size is %d, but tried to store value of size %d.",
                                MAX_TWO_BYTE_VALUE_SIZE, valueSize));
                    }
                    firstByte |= FLAG_ADDITIONAL_VALUE_SIZE;
                }
                cursor.putByte(firstByte);

                if (needsAdditionalValueSize) {
                    // Assuming no value size larger than 16k
                    cursor.putByte((byte) (valueSize >> SHIFT_LSB_VALUE_SIZE));
                }
            }
        }
    }

    public static void putOffloadMarker(PageCursor cursor) {
        cursor.putByte((byte) FLAG_ADDITIONAL_KEY_SIZE);
        cursor.putByte((byte) FLAG_SECOND_BYTE_OFFLOAD);
    }

    public static long readKeyValueSize(PageCursor cursor) {
        byte firstByte = cursor.getByte();
        boolean hasTombstone = hasTombstone(firstByte);
        boolean hasAdditionalKeySize = (firstByte & FLAG_ADDITIONAL_KEY_SIZE) != 0;
        boolean hasValueSize = (firstByte & FLAG_HAS_VALUE_SIZE) != 0;
        int keySizeLsb = firstByte & MASK_ONE_BYTE_KEY_SIZE;
        long keySize;
        if (hasAdditionalKeySize) {
            byte secondByte = cursor.getByte();
            if (hasOffload(secondByte)) {
                return (hasTombstone ? FLAG_READ_TOMBSTONE : 0) | FLAG_READ_OFFLOAD;
            }
            int keySizeMsb = secondByte & 0xFF;
            keySize = (keySizeMsb << SHIFT_LSB_KEY_SIZE) | keySizeLsb;
        } else {
            keySize = keySizeLsb;
        }

        long valueSize;
        if (hasValueSize) {
            byte firstValueByte = cursor.getByte();
            int valueSizeLsb = firstValueByte & MASK_ONE_BYTE_VALUE_SIZE;
            boolean hasAdditionalValueSize = (firstValueByte & FLAG_ADDITIONAL_VALUE_SIZE) != 0;
            if (hasAdditionalValueSize) {
                int valueSizeMsb = cursor.getByte() & 0xFF;
                valueSize = (valueSizeMsb << SHIFT_LSB_VALUE_SIZE) | valueSizeLsb;
            } else {
                valueSize = valueSizeLsb;
            }
        } else {
            valueSize = 0;
        }

        return (hasTombstone ? FLAG_READ_TOMBSTONE : 0) | (keySize << Integer.SIZE) | valueSize;
    }

    public static int extractValueSize(long keyValueSize) {
        return (int) keyValueSize;
    }

    public static int extractKeySize(long keyValueSize) {
        return (int) ((keyValueSize & ~(FLAG_READ_TOMBSTONE | FLAG_READ_OFFLOAD)) >>> Integer.SIZE);
    }

    public static int getOverhead(int keySize, int valueSize, boolean offload) {
        if (offload) {
            return 2 + SIZE_OFFLOAD_ID;
        }
        return 1
                + (keySize > MASK_ONE_BYTE_KEY_SIZE ? 1 : 0)
                + (valueSize > 0 ? 1 : 0)
                + (valueSize > MASK_ONE_BYTE_VALUE_SIZE ? 1 : 0);
    }

    static boolean extractTombstone(long keyValueSize) {
        return (keyValueSize & FLAG_READ_TOMBSTONE) != 0;
    }

    static boolean extractOffload(long keyValueSize) {
        return (keyValueSize & FLAG_READ_OFFLOAD) != 0;
    }

    /**
     * Put a tombstone into key size.
     * @param cursor on offset to key size where tombstone should be put.
     */
    static void putTombstone(PageCursor cursor) {
        int offset = cursor.getOffset();
        byte firstByte = cursor.getByte();
        firstByte = withTombstoneFlag(firstByte);
        cursor.setOffset(offset);
        cursor.putByte(firstByte);
    }

    /**
     * Check read key size for tombstone.
     * @return True if read key size has tombstone.
     */
    private static boolean hasTombstone(byte firstKeySizeByte) {
        return (firstKeySizeByte & FLAG_FIRST_BYTE_TOMBSTONE) != 0;
    }

    private static byte withTombstoneFlag(byte firstByte) {
        assert (firstByte & FLAG_FIRST_BYTE_TOMBSTONE) == 0
                : "First key size byte " + firstByte + " is too large to fit tombstone.";
        return (byte) (firstByte | FLAG_FIRST_BYTE_TOMBSTONE);
    }

    private static boolean hasOffload(long secondByte) {
        return (secondByte & FLAG_SECOND_BYTE_OFFLOAD) != 0;
    }

    static long readOffloadId(PageCursor cursor) {
        return cursor.getLong();
    }

    static void putOffloadId(PageCursor cursor, long offloadId) {
        cursor.putLong(offloadId);
    }

    static long offloadIdAt(PageCursor cursor) {
        long keyValueSize = readKeyValueSize(cursor);
        boolean offload = extractOffload(keyValueSize);
        if (offload) {
            return DynamicSizeUtil.readOffloadId(cursor);
        }
        return TreeNodeUtil.NO_OFFLOAD_ID;
    }

    /**
     * Redirects cursor by reading new offset from at the location specified by offset parameter
     *
     * @param cursor       - cursor
     * @param offset       - offset where to look for the target offset
     * @param headerLength - header length for the range check
     * @param payloadSize  - payload size for the range check
     */
    static void redirectCursor(PageCursor cursor, int offset, int headerLength, int payloadSize) {
        cursor.setOffset(offset);

        // Read actual offset to key
        int targetOffset = getUnsignedShort(cursor);

        // Verify offset is reasonable
        if (targetOffset >= payloadSize || targetOffset < headerLength) {
            cursor.setCursorException(format(
                    "Tried to read key on offset=%d, headerLength=%d, payloadSize=%d, offset=%d",
                    targetOffset, headerLength, payloadSize, offset));
            return;
        }

        // Set cursor to actual offset
        cursor.setOffset(targetOffset);
    }

    static int getDeadSpace(PageCursor cursor) {
        return getUnsignedShort(cursor, BYTE_POS_DEAD_SPACE);
    }

    static void setAllocOffset(PageCursor cursor, int allocOffset) {
        putUnsignedShort(cursor, BYTE_POS_ALLOC_OFFSET, allocOffset);
    }

    static int getAllocOffset(PageCursor cursor) {
        return getUnsignedShort(cursor, BYTE_POS_ALLOC_OFFSET);
    }

    @VisibleForTesting
    static void setDeadSpace(PageCursor cursor, int deadSpace) {
        putUnsignedShort(cursor, BYTE_POS_DEAD_SPACE, deadSpace);
    }

    static int getAllocSpace(PageCursor cursor, int endOfOffsetArray) {
        int allocOffset = getAllocOffset(cursor);
        return allocOffset - endOfOffsetArray;
    }

    static Overflow calculateOverflow(int neededSpace, int deadSpace, int allocSpace) {
        return neededSpace <= allocSpace
                ? Overflow.NO
                : neededSpace <= allocSpace + deadSpace ? Overflow.NO_NEED_DEFRAG : Overflow.YES;
    }

    /**
     * Given the offsets and sizes, moves bytes located at those offsets to the right boundary and updates offset array using posToOffsetFunction.
     *
     * This function does second and third steps of node defragmetation:
     * The goal is to compact all alive keys in the node by reusing the space occupied by dead keys.
     *
     * BEFORE
     * [8][X][1][3][X][2][X][7][5]
     *
     * AFTER
     * .........[8][1][3][2][7][5]
     * ^ Reclaimed space
     *
     * It works in 3 simple steps:
     * 1. collect all alive blocks with their sizes
     * 2. move all alive blocks to the rightmost position
     * 3. update offsets in offsets array
     *
     * See {@link DynamicSizeUtil#recordAliveBlocks} for the first step
     *
     * @param cursor - cursor pointing to the node
     * @param count - number of elements in offsets and sizes arrays
     * @param offsets - offsets to move
     * @param sizes - corresponding numbers of bytes at offsets
     * @param rightBoundary - right boundary
     * @param posToOffsetFunction - function to map position in offset array to offset
     */
    static void compactToRight(
            PageCursor cursor,
            int count,
            int[] offsets,
            int[] sizes,
            int rightBoundary,
            IntToIntFunction posToOffsetFunction) {
        var remappedOffsets = compactRight(cursor, count, offsets, sizes, rightBoundary);
        remapOffsets(cursor, count, remappedOffsets, posToOffsetFunction);
    }

    private static void remapOffsets(
            PageCursor cursor, int keyCount, IntIntHashMap remappedOffsets, IntToIntFunction posToOffsetFunction) {
        for (int pos = 0; pos < keyCount; pos++) {
            int keyPosOffset = posToOffsetFunction.valueOf(pos);
            cursor.setOffset(keyPosOffset);
            int keyOffset = getUnsignedShort(cursor);
            cursor.setOffset(keyPosOffset);
            assert remappedOffsets.containsKey(keyOffset)
                    : "missing mapping for offset " + keyOffset + " at pos " + pos + " key count " + keyCount
                            + " all mappings " + remappedOffsets;
            putUnsignedShort(cursor, remappedOffsets.get(keyOffset));
        }
    }

    private static IntIntHashMap compactRight(
            PageCursor cursor, int keyCount, int[] offsets, int[] sizes, int rightBoundary) {
        int targetOffset = rightBoundary;
        var remappedOffsets = new IntIntHashMap();
        for (int index = keyCount - 1; index >= 0; index--) {
            // move sizes[index] bytes from offsets[index] to the right so the end of the entry is at the targetOffset
            var entrySize = sizes[index];
            var sourceOffset = offsets[index];
            targetOffset -= entrySize;
            if (sourceOffset != targetOffset) {
                cursor.copyTo(sourceOffset, cursor, targetOffset, entrySize);
            }
            remappedOffsets.put(sourceOffset, targetOffset);
        }

        // Update allocOffset¸
        int prevAllocOffset = getAllocOffset(cursor);
        setAllocOffset(cursor, targetOffset);

        // Zero pad reclaimed area
        zeroPad(cursor, prevAllocOffset, targetOffset - prevAllocOffset);
        return remappedOffsets;
    }

    private static void zeroPad(PageCursor fromCursor, int fromOffset, int lengthInBytes) {
        fromCursor.setOffset(fromOffset);
        fromCursor.putBytes(lengthInBytes, (byte) 0);
    }

    static void recordAliveBlocks(PageCursor cursor, int keyCount, int[] offsets, int[] sizes, int payloadSize) {
        int index = 0;
        int currentOffset = getAllocOffset(cursor);
        while (currentOffset < payloadSize && index < keyCount) {
            cursor.setOffset(currentOffset);
            long keyValueSize = readKeyValueSize(cursor);
            int keySize = extractKeySize(keyValueSize);
            int valueSize = extractValueSize(keyValueSize);
            boolean offload = extractOffload(keyValueSize);
            boolean dead = extractTombstone(keyValueSize);

            var entrySize = keySize + valueSize + getOverhead(keySize, valueSize, offload);
            if (!dead) {
                offsets[index] = currentOffset;
                sizes[index] = entrySize;
                index++;
            }
            currentOffset += entrySize;
        }
        assert index == keyCount : "expected " + keyCount + " alive blocks, found only " + index;
    }

    @VisibleForTesting
    public static int keyValueSizeCapFromPageSize(int pageSize) {
        return Math.min(FIXED_MAX_KEY_VALUE_SIZE_CAP, OffloadStoreImpl.keyValueSizeCapFromPageSize(pageSize));
    }

    /**
     * Inline key value size cap is calculated based of payload size and capped with max supported size of key for
     * inlined encoding.
     */
    static int inlineKeyValueSizeCap(int payloadSize) {
        int totalOverhead = OFFSET_SIZE + MAX_SIZE_KEY_VALUE_SIZE;
        int capToFitNumberOfEntriesPerPage =
                (payloadSize - HEADER_LENGTH_DYNAMIC) / LEAST_NUMBER_OF_ENTRIES_PER_PAGE - totalOverhead;
        return Math.min(MAX_TWO_BYTE_KEY_SIZE, capToFitNumberOfEntriesPerPage);
    }

    static void validateInlineCap(int inlineKeyValueSizeCap, int payloadSize) {
        if (inlineKeyValueSizeCap < MINIMUM_ENTRY_SIZE_CAP) {
            throw new MetadataMismatchException(format(
                    "We need to fit at least %d key-value entries per page in leaves. To do that a key-value entry can be at most %dB "
                            + "with current page size of %dB. We require this cap to be at least %dB.",
                    LEAST_NUMBER_OF_ENTRIES_PER_PAGE, inlineKeyValueSizeCap, payloadSize, MINIMUM_ENTRY_SIZE_CAP));
        }
    }
}
