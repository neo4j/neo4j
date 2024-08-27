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
package org.neo4j.kernel.impl.store;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

import java.util.Arrays;
import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Defines valid property types.
 */
public enum PropertyType {
    BOOL(1) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return Values.booleanValue(getValue(block.getSingleValueLong()));
        }

        private boolean getValue(long propBlock) {
            return (propBlock & 0x1) == 1;
        }
    },
    BYTE(2) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return Values.byteValue(block.getSingleValueByte());
        }
    },
    SHORT(3) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return Values.shortValue(block.getSingleValueShort());
        }
    },
    CHAR(4) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return Values.charValue((char) block.getSingleValueShort());
        }
    },
    INT(5) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return Values.intValue(block.getSingleValueInt());
        }
    },
    LONG(6) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            long firstBlock = block.getSingleValueBlock();
            long value = valueIsInlined(firstBlock) ? (block.getSingleValueLong() >>> 1) : block.getValueBlocks()[1];
            return Values.longValue(value);
        }

        private boolean valueIsInlined(long firstBlock) {
            // [][][][][   i,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
            return (firstBlock & 0x10000000L) > 0;
        }

        @Override
        public int calculateNumberOfBlocksUsed(long firstBlock) {
            return valueIsInlined(firstBlock) ? 1 : 2;
        }
    },
    FLOAT(7) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return Values.floatValue(Float.intBitsToFloat(block.getSingleValueInt()));
        }
    },
    DOUBLE(8) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return Values.doubleValue(Double.longBitsToDouble(block.getValueBlocks()[1]));
        }

        @Override
        public int calculateNumberOfBlocksUsed(long firstBlock) {
            return 2;
        }
    },
    STRING(9) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors storeCursors, MemoryTracker memoryTracker) {
            return store.getTextValueFor(block, storeCursors, memoryTracker);
        }

        @Override
        public byte[] readDynamicRecordHeader(byte[] recordBytes) {
            return EMPTY_BYTE_ARRAY;
        }
    },
    ARRAY(10) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return store.getArrayFor(block, cursors, memoryTracker);
        }

        @Override
        public byte[] readDynamicRecordHeader(byte[] recordBytes) {
            byte itemType = recordBytes[0];
            if (itemType == STRING.byteValue()) {
                return headOf(recordBytes, DynamicArrayStore.STRING_HEADER_SIZE);
            } else if (itemType <= DOUBLE.byteValue()) {
                return headOf(recordBytes, DynamicArrayStore.NUMBER_HEADER_SIZE);
            } else if (itemType == GEOMETRY.byteValue()) {
                return headOf(recordBytes, DynamicArrayStore.GEOMETRY_HEADER_SIZE);
            } else if (itemType == TEMPORAL.byteValue()) {
                return headOf(recordBytes, DynamicArrayStore.TEMPORAL_HEADER_SIZE);
            }
            throw new IllegalArgumentException("Unknown array type " + itemType);
        }

        private byte[] headOf(byte[] bytes, int length) {
            return Arrays.copyOf(bytes, length);
        }
    },
    SHORT_STRING(11) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return LongerShortString.decode(block);
        }

        @Override
        public int calculateNumberOfBlocksUsed(long firstBlock) {
            return LongerShortString.calculateNumberOfBlocksUsed(firstBlock);
        }
    },
    SHORT_ARRAY(12) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return ShortArray.decode(block);
        }

        @Override
        public int calculateNumberOfBlocksUsed(long firstBlock) {
            return ShortArray.calculateNumberOfBlocksUsed(firstBlock);
        }
    },
    GEOMETRY(13) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return GeometryType.decode(block);
        }

        @Override
        public int calculateNumberOfBlocksUsed(long firstBlock) {
            return GeometryType.calculateNumberOfBlocksUsed(firstBlock);
        }
    },
    TEMPORAL(14) {
        @Override
        public Value value(
                PropertyBlock block, PropertyStore store, StoreCursors cursors, MemoryTracker memoryTracker) {
            return TemporalType.decode(block);
        }

        @Override
        public int calculateNumberOfBlocksUsed(long firstBlock) {
            return TemporalType.calculateNumberOfBlocksUsed(firstBlock);
        }
    };

    public static final int BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING = -1;

    // TODO In wait of a better place
    private static final int PAYLOAD_SIZE = PropertyRecordFormat.DEFAULT_PAYLOAD_SIZE;

    private final int type;

    PropertyType(int type) {
        this.type = type;
    }

    /**
     * Returns an int value representing the type.
     *
     * @return The int value for this property type
     */
    public int intValue() {
        return type;
    }

    /**
     * Returns a byte value representing the type. As long as there are
     * &lt 128 PropertyTypes, this should be equal to intValue(). When this
     * statement no longer holds, this method should be removed.
     *
     * @return The byte value for this property type
     */
    public byte byteValue() {
        return (byte) type;
    }

    public abstract Value value(
            PropertyBlock block, PropertyStore store, StoreCursors storeCursors, MemoryTracker memoryTracker);

    public static PropertyType getPropertyTypeOrNull(long propBlock) {
        // [][][][][    ,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        int type = typeIdentifier(propBlock);
        return switch (type) {
            case 1 -> BOOL;
            case 2 -> BYTE;
            case 3 -> SHORT;
            case 4 -> CHAR;
            case 5 -> INT;
            case 6 -> LONG;
            case 7 -> FLOAT;
            case 8 -> DOUBLE;
            case 9 -> STRING;
            case 10 -> ARRAY;
            case 11 -> SHORT_STRING;
            case 12 -> SHORT_ARRAY;
            case 13 -> GEOMETRY;
            case 14 -> TEMPORAL;
            default -> null;
        };
    }

    private static int typeIdentifier(long propBlock) {
        return (int) ((propBlock & 0x000000000F000000L) >> 24);
    }

    public static PropertyType getPropertyTypeOrThrow(long propBlock) {
        PropertyType type = getPropertyTypeOrNull(propBlock);
        if (type == null) {
            throw new InvalidRecordException("Unknown property type: " + typeIdentifier(propBlock)
                    + " (from property block " + Long.toHexString(propBlock) + ").");
        }
        return type;
    }

    // TODO In wait of a better place
    public static int getPayloadSize() {
        return PAYLOAD_SIZE;
    }

    // TODO In wait of a better place
    public static int getPayloadSizeLongs() {
        return PAYLOAD_SIZE >>> 3;
    }

    public int calculateNumberOfBlocksUsed(long firstBlock) {
        return 1;
    }

    public byte[] readDynamicRecordHeader(byte[] recordBytes) {
        throw new UnsupportedOperationException();
    }
}
