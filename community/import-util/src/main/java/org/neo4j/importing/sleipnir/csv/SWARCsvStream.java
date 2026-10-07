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
package org.neo4j.importing.sleipnir.csv;

import static java.lang.Long.numberOfTrailingZeros;
import static org.neo4j.importing.sleipnir.csv.SWARUtil.broadcast;
import static org.neo4j.importing.sleipnir.csv.SWARUtil.eqMask;
import static org.neo4j.importing.sleipnir.csv.SWARUtil.prefixXor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 *
 * Escape bitmask. A bitmask of escape characters, e.g. '\'
 * Escaped bitmask. A bitmask of escaped characters, e.g. '"'
 */
public class SWARCsvStream implements CsvStream {
    protected static final byte ESCAPE_CHARACTER = '\\';
    protected static final byte NEW_LINE_CHARACTER = '\n';
    private static final long ESCAPE_NEEDLE = broadcast(ESCAPE_CHARACTER);
    private static final long NEW_LINE_NEEDLE = broadcast(NEW_LINE_CHARACTER);
    private static final long ODD_BITS = 0xAAAAAAAAAAAAAAAAL;

    protected final byte separatorCharacter;
    private final long separatorCharacterNeedle;
    protected final byte quoteCharacter;
    private final long quoteCharacterNeedle;
    protected final boolean legacyStyleEscape;

    /**
     * Carry over quote state.
     */
    private long prevIterInsideQuote;
    /**
     * Carry over escape state.
     */
    private long nextIsEscaped;

    public SWARCsvStream() {
        this(',', '"', false);
    }

    public SWARCsvStream(int separatorCharacter, int quoteCharacter, boolean legacyStyleEscape) {
        this.separatorCharacter = canBeRepresentedInByte(separatorCharacter, "separator");
        this.separatorCharacterNeedle = broadcast(separatorCharacter);
        this.quoteCharacter = canBeRepresentedInByte(quoteCharacter, "quote");
        this.quoteCharacterNeedle = broadcast(quoteCharacter);
        this.legacyStyleEscape = legacyStyleEscape;
    }

    @Override
    public int indexSeparators(ByteBuffer buffer, int baseOffset, int[] destination) {
        assert buffer.order() == ByteOrder.LITTLE_ENDIAN;

        int indexCursor = 0; // Current append index to the "offsets" array

        // Align to 64 byte iterations
        int tail = buffer.limit() & (Long.SIZE - 1);
        int limit = buffer.limit() - tail;
        int offset = 0;
        while (offset < limit) {
            long l0 = buffer.getLong(offset);
            long l1 = buffer.getLong(offset + 8);
            long l2 = buffer.getLong(offset + 16);
            long l3 = buffer.getLong(offset + 24);
            long l4 = buffer.getLong(offset + 32);
            long l5 = buffer.getLong(offset + 40);
            long l6 = buffer.getLong(offset + 48);
            long l7 = buffer.getLong(offset + 56);

            long separator = eqMask(separatorCharacterNeedle, l0, l1, l2, l3, l4, l5, l6, l7);
            long end = eqMask(NEW_LINE_NEEDLE, l0, l1, l2, l3, l4, l5, l6, l7);
            long quote = eqMask(quoteCharacterNeedle, l0, l1, l2, l3, l4, l5, l6, l7);

            long escapedCharacters = 0;
            if (legacyStyleEscape) {
                long escape = eqMask(ESCAPE_NEEDLE, l0, l1, l2, l3, l4, l5, l6, l7);
                escapedCharacters = nextEscapedBitmask(escape);
            }

            long inString = inStringBitmask(quote, escapedCharacters);
            long index = (end | separator) & ~inString;
            indexCursor = extractIndexes(destination, indexCursor, baseOffset + offset, index);
            offset += 64;
        }

        buffer.position(limit);
        indexCursor = processTail(buffer, baseOffset, indexCursor, destination);

        return indexCursor;
    }

    @Override
    public void validateEnd() {
        if (nextIsEscaped != 0) {
            throw new IllegalStateException("CSV ended with an escape character.");
        }
        if (prevIterInsideQuote != 0) {
            throw new IllegalStateException("CSV ended without closing an open quote.");
        }
    }

    @Override
    public void reset() {
        nextIsEscaped = 0;
        prevIterInsideQuote = 0;
    }

    protected int processTail(ByteBuffer buffer, int baseOffset, int indexCursor, int[] destination) {
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (nextIsEscaped != 0) {
                nextIsEscaped = 0;
            } else if (legacyStyleEscape && b == ESCAPE_CHARACTER) {
                nextIsEscaped = 1;
            } else if (prevIterInsideQuote != 0) {
                if (b == quoteCharacter) {
                    prevIterInsideQuote = 0;
                }
            } else if (b == quoteCharacter) {
                prevIterInsideQuote = 1;
            } else if (b == separatorCharacter) {
                destination[indexCursor++] = baseOffset + buffer.position() - 1;
            } else if (b == NEW_LINE_CHARACTER) {
                destination[indexCursor++] = baseOffset + buffer.position() - 1;
            }
        }
        return indexCursor;
    }

    /**
     * @param escapedCharacters a bitmask where a 1 represents an escaped character.
     * @return a bitmask where a 1 represents that the character is inside a string.
     */
    protected long inStringBitmask(long quote, long escapedCharacters) {
        long mask = quote & ~escapedCharacters;
        long quotedMask = prefixXor(mask);

        quotedMask ^= prevIterInsideQuote;
        prevIterInsideQuote = quotedMask >> 63;
        return quotedMask;
    }

    /**
     * Get the next escaped character
     * @param escapeBitmask bitmask of escape characters
     * @return bitmask of escaped characters
     */
    protected long nextEscapedBitmask(long escapeBitmask) {
        long previousIsEscaped = nextIsEscaped;
        if (escapeBitmask == 0) {
            // No new escape characters
            nextIsEscaped = 0;
            return previousIsEscaped;
        }

        // If last iteration ended with an escape, we can ignore the first escape character in this iteration
        long escapeBitmap = escapeBitmask & ~previousIsEscaped;

        // Shift over by 1 so the bitmask is now representing the characters that can be escaped
        long maybeEscapedBitmap = escapeBitmap << 1;

        // A character is only escaped if it is proceeded by an odd number of escape charters, e.g. \\" is not an
        // escaped quotation mark.
        long escapedAndOddBitmap = (maybeEscapedBitmap | ODD_BITS) - escapeBitmap ^ ODD_BITS;

        // Carry over to next iteration.
        nextIsEscaped = (escapedAndOddBitmap & escapeBitmask) >>> 63;

        return escapedAndOddBitmap ^ (escapeBitmask | previousIsEscaped);
    }

    /**
     * Extract all separators from the {@code bitmap} to the destination {@code indexes}.
     *
     * @param indexes array to insert the offsets to.
     * @param indexesCursor current insert-point in {@code indexes}.
     * @param offset current offset of chunk.
     * @param bitmask bitmask with all separators.
     * @return the next {@code indexesCursor} to use.
     */
    protected static int extractIndexes(int[] indexes, int indexesCursor, int offset, long bitmask) {
        int cnt = Long.bitCount(bitmask);
        int nextBase = indexesCursor + cnt;
        // Unrolled loop to avoid miss predicted branches
        indexes[indexesCursor] = offset + numberOfTrailingZeros(bitmask);
        bitmask &= bitmask - 1; // Clear lowest bit
        indexes[indexesCursor + 1] = offset + numberOfTrailingZeros(bitmask);
        bitmask &= bitmask - 1;
        indexes[indexesCursor + 2] = offset + numberOfTrailingZeros(bitmask);
        bitmask &= bitmask - 1;
        indexes[indexesCursor + 3] = offset + numberOfTrailingZeros(bitmask);
        bitmask &= bitmask - 1;
        indexes[indexesCursor + 4] = offset + numberOfTrailingZeros(bitmask);
        bitmask &= bitmask - 1;
        indexes[indexesCursor + 5] = offset + numberOfTrailingZeros(bitmask);
        bitmask &= bitmask - 1;
        indexes[indexesCursor + 6] = offset + numberOfTrailingZeros(bitmask);
        bitmask &= bitmask - 1;
        indexes[indexesCursor + 7] = offset + numberOfTrailingZeros(bitmask);
        bitmask &= bitmask - 1;
        if (cnt > 8) {
            indexes[indexesCursor + 8] = offset + numberOfTrailingZeros(bitmask);
            bitmask &= bitmask - 1;
            indexes[indexesCursor + 9] = offset + numberOfTrailingZeros(bitmask);
            bitmask &= bitmask - 1;
            indexes[indexesCursor + 10] = offset + numberOfTrailingZeros(bitmask);
            bitmask &= bitmask - 1;
            indexes[indexesCursor + 11] = offset + numberOfTrailingZeros(bitmask);
            bitmask &= bitmask - 1;
            indexes[indexesCursor + 12] = offset + numberOfTrailingZeros(bitmask);
            bitmask &= bitmask - 1;
            indexes[indexesCursor + 13] = offset + numberOfTrailingZeros(bitmask);
            bitmask &= bitmask - 1;
            indexes[indexesCursor + 14] = offset + numberOfTrailingZeros(bitmask);
            bitmask &= bitmask - 1;
            indexes[indexesCursor + 15] = offset + numberOfTrailingZeros(bitmask);
            bitmask &= bitmask - 1;
        }
        if (cnt > 16) {
            indexesCursor += 16;
            do {
                indexes[indexesCursor] = offset + numberOfTrailingZeros(bitmask);
                bitmask &= bitmask - 1;
                indexesCursor++;
            } while (bitmask != 0);
        }
        return nextBase;
    }

    private static byte canBeRepresentedInByte(int b, String field) {
        if ((b & 0xff) != b) {
            throw new IllegalArgumentException(
                    "'%s' is not supported as %s".formatted(new String(Character.toChars(b)), field));
        }
        return (byte) b;
    }
}
