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

import static org.neo4j.kernel.impl.index.schema.Types.SIZE_ARRAY_LENGTH;

import org.neo4j.graphdb.Vector;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.VectorValue;

class VectorArrayType extends AbstractArrayType<VectorValue> {
    // Affected key state:
    // long0Array: coordinateType+dimensions <msb>[1B coordinateType],[2B dimensions]<lsb>
    // byteArrayArray

    VectorArrayType(byte typeId) {
        super(
                ValueGroup.VECTOR_ARRAY,
                typeId,
                new VectorArrayElementComparator(),
                new VectorArrayElementValueFactory(),
                new VectorArrayElementWriter(),
                null,
                VectorValue[]::new,
                ValueWriter.ArrayType.VECTOR);
    }

    @Override
    int valueSize(GenericKey<?> state) {
        int size = SIZE_ARRAY_LENGTH;
        for (int i = 0; i < state.arrayLength; i++) {
            size += 3 + byteLengthOf(state, i);
        }
        return size;
    }

    @Override
    boolean readValue(PageCursor cursor, int size, GenericKey<?> into) {
        short length = cursor.getShort();
        if (!setArrayLengthWhenReading(into, cursor, length)) {
            return false;
        }
        into.beginArray(into.arrayLength, ValueWriter.ArrayType.VECTOR);
        into.long0Array = ensureBigEnough(into.long0Array, into.arrayLength);
        into.byteArrayArray = ensureBigEnough(into.byteArrayArray, into.arrayLength);
        for (int i = 0; i < into.arrayLength; i++) {
            into.long0Array[i] = PageCursorUtil.get3BInt(cursor) & 0xFFFFFF;
            int byteLength = byteLengthOf(into, i);
            into.byteArrayArray[i] = ensureBigEnough(into.byteArrayArray[i], byteLength);
            cursor.getBytes(into.byteArrayArray[i], 0, byteLength);
        }
        into.endArray();
        return true;
    }

    private static byte coordinateTypeOf(long header) {
        return (byte) (header >>> (Short.SIZE));
    }

    private static int dimensionsOf(long header) {
        return (int) (header & 0xFFFF);
    }

    private static int byteLengthOf(GenericKey<?> key, int i) {
        long header = key.long0Array[i];
        int dimensions = dimensionsOf(header);
        Vector.CoordinateType coordinateType = byteToCoordinateType(coordinateTypeOf(header));
        return VectorValue.bytesPerDimension(coordinateType) * dimensions;
    }

    @Override
    void copyValue(GenericKey<?> to, GenericKey<?> from, int arrayLength) {
        to.long0Array = ensureBigEnough(to.long0Array, arrayLength);
        System.arraycopy(from.long0Array, 0, to.long0Array, 0, arrayLength);
        to.byteArrayArray = ensureBigEnough(to.byteArrayArray, arrayLength);
        for (int i = 0; i < arrayLength; i++) {
            to.byteArrayArray[i] = ensureBigEnough(to.byteArrayArray[i], from.byteArrayArray[i].length);
            System.arraycopy(from.byteArrayArray[i], 0, to.byteArrayArray[i], 0, from.byteArrayArray[i].length);
        }
    }

    @Override
    void initializeArray(GenericKey<?> key, int length, ValueWriter.ArrayType arrayType) {
        key.long0Array = ensureBigEnough(key.long0Array, length);
        key.byteArrayArray = ensureBigEnough(key.byteArrayArray, length);
    }

    static void write(GenericKey<?> key, int offset, Vector.CoordinateType coordinateType, int dimension, byte[] data) {
        key.long0Array[offset] = dimension;
        key.long0Array[offset] |= (long) coordinateTypeToByte(coordinateType) << (Short.SIZE);
        key.byteArrayArray[offset] = data;
    }

    private static byte coordinateTypeToByte(Vector.CoordinateType coordinateType) {
        return (byte)
                switch (coordinateType) {
                    case INTEGER8 -> 0;
                    case INTEGER16 -> 1;
                    case INTEGER32 -> 2;
                    case INTEGER64 -> 3;
                    case FLOAT32 -> 4;
                    case FLOAT64 -> 5;
                };
    }

    private static Vector.CoordinateType byteToCoordinateType(byte coordinateType) {
        return switch (coordinateType) {
            case 0 -> Vector.CoordinateType.INTEGER8;
            case 1 -> Vector.CoordinateType.INTEGER16;
            case 2 -> Vector.CoordinateType.INTEGER32;
            case 3 -> Vector.CoordinateType.INTEGER64;
            case 4 -> Vector.CoordinateType.FLOAT32;
            case 5 -> Vector.CoordinateType.FLOAT64;
            default -> throw new IllegalArgumentException("Invalid coordinate type: " + coordinateType);
        };
    }

    private static class VectorArrayElementComparator implements ArrayElementComparator {
        @Override
        public int compare(GenericKey<?> o1, GenericKey<?> o2, int i) {
            // first compare coordinateType, then dimensions, then arrays themselves
            byte coordinateTypeId = coordinateTypeOf(o1.long0Array[i]);
            int coordinateTypeComparison = Byte.compare(coordinateTypeId, coordinateTypeOf(o2.long0Array[i]));
            if (coordinateTypeComparison != 0) {
                return coordinateTypeComparison;
            }
            int dimensionsComparison = Integer.compare(dimensionsOf(o1.long0Array[i]), dimensionsOf(o2.long0Array[i]));
            if (dimensionsComparison != 0) {
                return dimensionsComparison;
            }
            Vector.CoordinateType coordinateType = byteToCoordinateType(coordinateTypeId);
            int numBytes = byteLengthOf(o1, i);
            byte[] o1Bytes = o1.byteArrayArray[i];
            byte[] o2Bytes = o2.byteArrayArray[i];
            return switch (coordinateType) {
                case INTEGER8 -> VectorKeyType.Int8VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case INTEGER16 -> VectorKeyType.Int16VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case INTEGER32 -> VectorKeyType.Int32VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case INTEGER64 -> VectorKeyType.Int64VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case FLOAT32 -> VectorKeyType.Float32VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
                case FLOAT64 -> VectorKeyType.Float64VectorKey.compareBytes(o1Bytes, o2Bytes, numBytes);
            };
        }
    }

    private static class VectorArrayElementValueFactory implements ArrayElementValueFactory<VectorValue> {
        @Override
        public VectorValue from(GenericKey<?> k, int i) {
            int dimensions = dimensionsOf(k.long0Array[i]);
            return switch (byteToCoordinateType(coordinateTypeOf(k.long0Array[i]))) {
                case INTEGER8 -> VectorKeyType.Int8VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case INTEGER16 -> VectorKeyType.Int16VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case INTEGER32 -> VectorKeyType.Int32VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case INTEGER64 -> VectorKeyType.Int64VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case FLOAT32 -> VectorKeyType.Float32VectorKey.asValue(dimensions, k.byteArrayArray[i]);
                case FLOAT64 -> VectorKeyType.Float64VectorKey.asValue(dimensions, k.byteArrayArray[i]);
            };
        }
    }

    private static class VectorArrayElementWriter implements ArrayElementWriter {
        @Override
        public void write(PageCursor c, GenericKey<?> k, int i) {
            PageCursorUtil.put3BInt(c, (int) k.long0Array[i]);
            int byteLength = byteLengthOf(k, i);
            c.putBytes(k.byteArrayArray[i], 0, byteLength);
        }
    }
}
