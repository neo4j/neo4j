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
package org.neo4j.kernel.impl.store.record;

/**
 * Various constants used in records for different stores.
 */
public enum Record {
    /**
     * Generic value of a reference not pointing to anything.
     */
    NULL_REFERENCE((byte) -1, -1),

    NOT_IN_USE((byte) 0, 0),
    IN_USE((byte) 1, 1),
    RESERVED((byte) -1, -1),
    NO_NEXT_PROPERTY(NULL_REFERENCE),
    NO_PREVIOUS_PROPERTY(NULL_REFERENCE),
    NO_NEXT_RELATIONSHIP(NULL_REFERENCE),
    NO_PREV_RELATIONSHIP(NULL_REFERENCE),
    NO_NEXT_BLOCK(NULL_REFERENCE),

    NODE_PROPERTY((byte) 0, 0),
    REL_PROPERTY((byte) 2, 2),

    NO_LABELS_FIELD((byte) 0, 0);

    public static final int CREATED_IN_TX = 0b0000_0010;
    public static final int REQUIRE_SECONDARY_UNIT = 0b0000_0100;
    public static final int HAS_SECONDARY_UNIT = 0b0000_1000;
    public static final int USES_FIXED_REFERENCE_FORMAT = 0b0001_0000;
    // Named a bit more generically and elusive because this flag is used for different things depending on which type
    // of record it is
    public static final int ADDITIONAL_FLAG_1 = 0b0010_0000;
    public static final int ADDITIONAL_FLAG_2 = 0b0100_0000;
    public static final int ADDITIONAL_FLAG_3 = 0b1000_0000;

    // Beware using these flags together and with ADDITIONAL_FLAG_*
    public static final int SECONDARY_UNIT_CREATED_IN_TX = ADDITIONAL_FLAG_1;
    public static final int TOKEN_INTERNAL = ADDITIONAL_FLAG_1;
    public static final int DYNAMIC_RECORD_START_RECORD = ADDITIONAL_FLAG_1;
    public static final int PROPERTY_OWNED_BY_NODE = ADDITIONAL_FLAG_2;
    public static final int PROPERTY_OWNED_BY_RELATIONSHIP = ADDITIONAL_FLAG_3;

    // not to use within the same byte with IN_USE and CREATED_IN_TX
    public static final byte RELATIONSHIP_FIRST_IN_FIRST_CHAIN = 0b0000_0001;
    public static final byte RELATIONSHIP_FIRST_IN_SECOND_CHAIN = 0b0000_0010;

    // not to use within the same byte with other flags
    public static final byte RELATIONSHIP_GROUP_EXTERNAL_DEGREES_OUT = 0b0000_0001;
    public static final byte RELATIONSHIP_GROUP_EXTERNAL_DEGREES_IN = 0b0000_0010;
    public static final byte RELATIONSHIP_GROUP_EXTERNAL_DEGREES_LOOP = 0b0000_0100;

    private final byte byteValue;
    private final int intValue;

    Record(Record from) {
        this(from.byteValue, from.intValue);
    }

    Record(byte byteValue, int intValue) {
        this.byteValue = byteValue;
        this.intValue = intValue;
    }

    /**
     * Returns a byte value representation for this record type.
     *
     * @return The byte value for this record type
     */
    public byte byteValue() {
        return byteValue;
    }

    /**
     * Returns a int value representation for this record type.
     *
     * @return The int value for this record type
     */
    public int intValue() {
        return intValue;
    }

    public long longValue() {
        return intValue;
    }

    public boolean is(long value) {
        return value == intValue;
    }

    public static boolean isNull(long id) {
        return NULL_REFERENCE.is(id);
    }
}
