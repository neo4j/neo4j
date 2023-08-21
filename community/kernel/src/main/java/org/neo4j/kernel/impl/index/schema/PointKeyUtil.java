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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.io.pagecache.PageCursor;

public class PointKeyUtil {
    static final int SIZE_GEOMETRY_COORDINATE = Long.BYTES; /* one coordinate */
    static final int SIZE_GEOMETRY_HEADER = 3; /* 2b crsTableId+ 3b dimensions + 1b dimensions reserve + 18b crsCode  */
    static final int SIZE_GEOMETRY_DERIVED_SPACE_FILLING_CURVE_VALUE = Long.BYTES; /* raw space filling curve value */

    private static final int MASK_CODE = 0b00000011_11111111_11111111;
    private static final int MASK_DIMENSIONS_READ = 0b00011100_00000000_00000000;
    //                                                  ^ this bit is reserved for future expansion of number of
    // dimensions
    private static final int MASK_TABLE_READ = 0b11000000_00000000_00000000;
    private static final int SHIFT_DIMENSIONS = Integer.bitCount(MASK_CODE);
    private static final int SHIFT_TABLE =
            SHIFT_DIMENSIONS + 1 /*the reserved dimension bit*/ + Integer.bitCount(MASK_DIMENSIONS_READ);
    private static final int MASK_TABLE_PUT = MASK_TABLE_READ >>> SHIFT_TABLE;
    private static final int MASK_DIMENSIONS_PUT = MASK_DIMENSIONS_READ >>> SHIFT_DIMENSIONS;

    /**
     * Write compact header containing crs table id, crs code and dimensions
     */
    static void writeHeader(PageCursor cursor, long crsTableId, long crsCode, long dimensions) {
        assertValueWithin(crsTableId, MASK_TABLE_PUT, "tableId");
        assertValueWithin(crsCode, MASK_CODE, "code");
        assertValueWithin(dimensions, MASK_DIMENSIONS_PUT, "dimensions");
        int header = (int) ((crsTableId << SHIFT_TABLE) | (dimensions << SHIFT_DIMENSIONS) | crsCode);
        put3BInt(cursor, header);
    }

    /**
     * Read compact header. Extract content using methods in this class.
     */
    static int readHeader(PageCursor cursor) {
        return read3BInt(cursor);
    }

    /**
     * Extract coordinate reference system table id from {@link #readHeader(PageCursor) read header}.
     */
    static int crsTableId(int header) {
        return (header & MASK_TABLE_READ) >>> SHIFT_TABLE;
    }

    /**
     * Extract coordinate reference system code from {@link #readHeader(PageCursor) read header}.
     */
    static int crsCode(int header) {
        return header & MASK_CODE;
    }

    /**
     * Extract dimensions from {@link #readHeader(PageCursor) read header}.
     */
    static int dimensions(int header) {
        return (header & MASK_DIMENSIONS_READ) >>> SHIFT_DIMENSIONS;
    }

    private static int read3BInt(PageCursor cursor) {
        int low = cursor.getShort() & 0xFFFF;
        int high = cursor.getByte() & 0xFF;
        return high << Short.SIZE | low;
    }

    private static void put3BInt(PageCursor cursor, int value) {
        cursor.putShort((short) value);
        cursor.putByte((byte) (value >>> Short.SIZE));
    }

    private static void assertValueWithin(long value, int maskAllowed, String name) {
        if ((value & ~maskAllowed) != 0) {
            throw new IllegalArgumentException("Expected 0 < " + name + " <= " + maskAllowed + ", but was " + value);
        }
    }
}
