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
package org.neo4j.importing.sleipnir.csv.simd;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;
import org.neo4j.importing.sleipnir.csv.SWARCsvStream;

public class SIMDCsvStream extends SWARCsvStream {
    private static final VectorSpecies<Byte> SIMD32 = ByteVector.SPECIES_256;

    public SIMDCsvStream(int separatorCharacter, int quoteCharacter, boolean legacyStyleEscape) {
        super(separatorCharacter, quoteCharacter, legacyStyleEscape);
    }

    @Override
    public int indexSeparators(ByteBuffer buffer, int baseOffset, int[] destination) {
        MemorySegment memorySegment = MemorySegment.ofBuffer(buffer);
        byte separatorCharacter = this.separatorCharacter;
        byte quoteCharacter = this.quoteCharacter;

        int indexCursor = 0; // Current append index to the "offsets" array

        int size = (int) memorySegment.byteSize();
        int tail = size & (Long.SIZE - 1);
        int limit = size - tail;
        int offset = 0;
        while (offset < limit) {
            ByteVector lo = ByteVector.fromMemorySegment(SIMD32, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
            ByteVector hi = ByteVector.fromMemorySegment(SIMD32, memorySegment, offset + 32, ByteOrder.LITTLE_ENDIAN);

            long separator = getMask(lo, hi, separatorCharacter);
            long end = getMask(lo, hi, NEW_LINE_CHARACTER);
            long quote = getMask(lo, hi, quoteCharacter);

            long escapedCharacters = 0;
            if (legacyStyleEscape) {
                long escape = getMask(lo, hi, ESCAPE_CHARACTER);
                escapedCharacters = nextEscapedBitmask(escape);
            }

            long inString = inStringBitmask(quote, escapedCharacters);
            long index = (end | separator) & ~inString;
            indexCursor = extractIndexes(destination, indexCursor, baseOffset + offset, index);

            offset += 64;
        }

        buffer.position(limit);
        return processTail(buffer, baseOffset, indexCursor, destination);
    }

    private static long getMask(ByteVector lo, ByteVector hi, byte m) {
        ByteVector mask = ByteVector.broadcast(SIMD32, m);
        return lo.eq(mask).toLong() | (hi.eq(mask).toLong() << 32);
    }
}
