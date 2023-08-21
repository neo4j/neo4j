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
package org.neo4j.packstream.io;

import static org.neo4j.packstream.io.LengthPrefix.NIBBLE;
import static org.neo4j.packstream.io.LengthPrefix.NONE;
import static org.neo4j.packstream.io.LengthPrefix.UINT16;
import static org.neo4j.packstream.io.LengthPrefix.UINT32;
import static org.neo4j.packstream.io.LengthPrefix.UINT8;

import java.util.List;

public enum TypeMarker {
    /**
     * Placeholder value which is returned when a given marker has yet to be assigned.
     */
    RESERVED(0x00, Type.RESERVED, NONE),

    TINY_INT(0x00, Type.INT, NONE),

    // Nibble Types
    TINY_STRING(0x80, Type.STRING, NIBBLE),
    TINY_LIST(0x90, Type.LIST, NIBBLE),
    TINY_MAP(0xA0, Type.MAP, NIBBLE),
    TINY_STRUCT(0xB0, Type.STRUCT, NIBBLE),

    // Static Values
    NULL(0xC0, Type.NONE, NONE),
    FALSE(0xC2, Type.BOOLEAN, NONE),
    TRUE(0xC3, Type.BOOLEAN, NONE),

    // Fixed Length Values
    INT8(0xC8, Type.INT, NONE),
    INT16(0xC9, Type.INT, NONE),
    INT32(0xCA, Type.INT, NONE),
    INT64(0xCB, Type.INT, NONE),

    FLOAT64(0xC1, Type.FLOAT, NONE),

    // Prefixed Types
    BYTES8(0xCC, Type.BYTES, UINT8),
    BYTES16(0xCD, Type.BYTES, UINT16),
    BYTES32(0xCE, Type.BYTES, UINT32),

    STRING8(0xD0, Type.STRING, UINT8),
    STRING16(0xD1, Type.STRING, UINT16),
    STRING32(0xD2, Type.STRING, UINT32),

    LIST8(0xD4, Type.LIST, UINT8),
    LIST16(0xD5, Type.LIST, UINT16),
    LIST32(0xD6, Type.LIST, UINT32),
    // FIXME: LIST_STREAM 0xD7

    MAP8(0xD8, Type.MAP, UINT8),
    MAP16(0xD9, Type.MAP, UINT16),
    MAP32(0xDA, Type.MAP, UINT32),
    // FIXME: MAP_STREAM 0xDB

    @Deprecated // Not documented
    STRUCT8(0xDC, Type.STRUCT, UINT8),
    @Deprecated // Not documented
    STRUCT16(0xDD, Type.STRUCT, UINT16),
// STRUCT_32 - 0xDE
;

    // Type Constants
    public static final short MARKER_MIN = 0x80;
    public static final short MARKER_MAX = 0xEF;
    public static final short VALID_MARKER_COUNT = MARKER_MAX - MARKER_MIN;

    // Internals
    private static final TypeMarker[] lookupTable = new TypeMarker[VALID_MARKER_COUNT];

    // Value Categories
    public static final List<TypeMarker> BOOLEAN_VALUES = List.of(FALSE, TRUE);

    public static final List<TypeMarker> BYTES_TYPES = List.of(BYTES8, BYTES16, BYTES32);
    public static final List<TypeMarker> INT_TYPES = List.of(TINY_INT, INT8, INT16, INT32, INT64);
    public static final List<TypeMarker> LIST_TYPES = List.of(TINY_LIST, LIST8, LIST16, LIST32);
    public static final List<TypeMarker> MAP_TYPES = List.of(TINY_MAP, MAP8, MAP16, MAP32);
    public static final List<TypeMarker> STRING_TYPES = List.of(TINY_STRING, STRING8, STRING16, STRING32);
    public static final List<TypeMarker> STRUCT_TYPES = List.of(TINY_STRUCT, STRUCT8, STRUCT16);

    private final short value;
    private final Type type;
    private final LengthPrefix lengthPrefix;

    TypeMarker(int value, Type type, LengthPrefix lengthPrefix) {
        this.value = (short) value;
        this.type = type;
        this.lengthPrefix = lengthPrefix;
    }

    static {
        for (var marker : values()) {
            if (marker == RESERVED || marker == TINY_INT) {
                continue;
            }

            lookupTable[marker.value - MARKER_MIN] = marker;

            // nibble based markers effectively exist in 15 different variations where each marker identifies the
            // length of the encoded value - they will simply be considered distinct markers here to speed things up
            if (marker.isNibbleMarker()) {
                for (var i = 1; i < 16; ++i) {
                    lookupTable[(marker.value ^ i) - MARKER_MIN] = marker;
                }
            }
        }
    }

    /**
     * Retrieves the type of given value based on its marker byte.
     *
     * @param marker a marker.
     * @return a value type.
     */
    public static TypeMarker byEncoded(short marker) {
        // TINY_INT uses the value range between 0xF0 and 0x80 (signed) to represents its values thus requiring a
        // shortcut
        // here as they are not part of the lookup table
        if (marker < MARKER_MIN || marker > MARKER_MAX) {
            return TINY_INT;
        }

        var offset = marker - MARKER_MIN;
        var type = lookupTable[offset];
        if (type == null) {
            return RESERVED;
        }
        return type;
    }

    /**
     * Decodes the length field within a nibble based marker.
     *
     * @param marker a marker byte.
     * @return a length prefix.
     */
    public static int decodeLengthNibble(short marker) {
        return marker & 0x0F;
    }

    /**
     * Encodes the length field within a nibble based marker.
     *
     * @param marker a marker.
     * @param length a length prefix.
     * @return a marker byte.
     */
    public static int encodeLengthNibble(TypeMarker marker, int length) {
        return marker.value ^ (length & 0xFF);
    }

    /**
     * Requires a given length parameter to be encodable within the restrictions of a given marker.
     *
     * @param marker a marker.
     * @param length a desired payload length.
     */
    public static void requireEncodableLength(TypeMarker marker, long length) {
        if (!marker.canEncodeLength(length)) {
            throw new IllegalArgumentException(
                    "Payload of " + length + " exceeds maximum of " + marker.lengthPrefix.getMaxValue());
        }
    }

    /**
     * Retrieves the version of this particular marker within the encoded Packstream format.
     *
     * @return an unsigned marker byte.
     */
    public short getValue() {
        return this.value;
    }

    /**
     * Retrieves the base type on which this particular marker relies.
     *
     * @return the base value type.
     */
    public Type getType() {
        return this.type;
    }

    /**
     * Retrieves the type of length prefix which is used by this marker (if any).
     *
     * @return a length prefix type.
     */
    public LengthPrefix getLengthPrefix() {
        return this.lengthPrefix;
    }

    /**
     * Shorthand function for {@link LengthPrefix#canEncode(long)}.
     *
     * @see LengthPrefix#canEncode(long) for detailed information.
     */
    public boolean canEncodeLength(long length) {
        if (!this.hasLengthPrefix()) {
            return false;
        }

        return this.lengthPrefix.canEncode(length);
    }

    /**
     * Identifies whether this particular type provides a length prefix identifying how many entries or how many bytes it contains.
     *
     * @return true if a length prefix is given, false otherwise.
     */
    public boolean hasLengthPrefix() {
        return this.lengthPrefix != NONE;
    }

    /**
     * Identifies whether this particular type stores the structure size within its 4 least significant bits thus requiring its type matching to be performed
     * via the 4 most significant bits.
     *
     * @return true if the marker is a nibble.
     */
    public boolean isNibbleMarker() {
        return this.lengthPrefix == NIBBLE;
    }
}
