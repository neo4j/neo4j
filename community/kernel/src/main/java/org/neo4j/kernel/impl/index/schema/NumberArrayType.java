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

import static org.neo4j.kernel.impl.index.schema.GenericKey.setCursorException;
import static org.neo4j.kernel.impl.index.schema.NumberType.numberKeySize;

import java.util.Arrays;
import java.util.StringJoiner;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

// Raw Number type is mostly for show as internally specific primitive int/long/short etc. arrays are created instead
class NumberArrayType extends AbstractArrayType<Number> {
    // Affected key state:
    // long0Array (value)
    // long1 (number type)

    NumberArrayType(byte typeId) {
        super(
                ValueGroup.NUMBER_ARRAY,
                typeId,
                (o1, o2, i) -> NumberType.compare(
                        o1.long0Array[i], o1.long1,
                        o2.long0Array[i], o2.long1),
                null,
                null,
                null,
                null,
                null);
    }

    @Override
    int valueSize(GenericKey<?> state) {
        return arrayKeySize(state, numberKeySize(state.long1)) + Types.SIZE_NUMBER_TYPE;
    }

    @Override
    void copyValue(GenericKey<?> to, GenericKey<?> from, int length) {
        to.long1 = from.long1;
        initializeArray(to, length);
        System.arraycopy(from.long0Array, 0, to.long0Array, 0, length);
    }

    @Override
    void initializeArray(GenericKey<?> key, int length, ValueWriter.ArrayType arrayType) {
        initializeArray(key, length);
        switch (arrayType) {
            case BYTE -> key.long1 = RawBits.BYTE;
            case SHORT -> key.long1 = RawBits.SHORT;
            case INT -> key.long1 = RawBits.INT;
            case LONG -> key.long1 = RawBits.LONG;
            case FLOAT -> key.long1 = RawBits.FLOAT;
            case DOUBLE -> key.long1 = RawBits.DOUBLE;
            default -> throw new IllegalArgumentException("Invalid number array type " + arrayType);
        }
    }

    private static void initializeArray(GenericKey<?> key, int length) {
        key.long0Array = ensureBigEnough(key.long0Array, length);
        // plain long1 for number type
    }

    @Override
    Value asValue(GenericKey<?> state) {
        byte numberType = (byte) state.long1;
        switch (numberType) {
            case RawBits.BYTE:
                byte[] byteArray = new byte[state.arrayLength];
                for (int i = 0; i < state.arrayLength; i++) {
                    byteArray[i] = (byte) state.long0Array[i];
                }
                return Values.byteArray(byteArray);
            case RawBits.SHORT:
                short[] shortArray = new short[state.arrayLength];
                for (int i = 0; i < state.arrayLength; i++) {
                    shortArray[i] = (short) state.long0Array[i];
                }
                return Values.shortArray(shortArray);
            case RawBits.INT:
                int[] intArray = new int[state.arrayLength];
                for (int i = 0; i < state.arrayLength; i++) {
                    intArray[i] = (int) state.long0Array[i];
                }
                return Values.intArray(intArray);
            case RawBits.LONG:
                return Values.longArray(Arrays.copyOf(state.long0Array, state.arrayLength));
            case RawBits.FLOAT:
                float[] floatArray = new float[state.arrayLength];
                for (int i = 0; i < state.arrayLength; i++) {
                    floatArray[i] = Float.intBitsToFloat((int) state.long0Array[i]);
                }
                return Values.floatArray(floatArray);
            case RawBits.DOUBLE:
                double[] doubleArray = new double[state.arrayLength];
                for (int i = 0; i < state.arrayLength; i++) {
                    doubleArray[i] = Double.longBitsToDouble(state.long0Array[i]);
                }
                return Values.doubleArray(doubleArray);
            default:
                throw new IllegalArgumentException("Unknown number type " + numberType);
        }
    }

    @Override
    void putValue(PageCursor cursor, GenericKey<?> state) {
        cursor.putByte((byte) state.long1);
        putArray(cursor, state, numberArrayElementWriter(state));
    }

    private static ArrayElementWriter numberArrayElementWriter(GenericKey<?> key) {
        return switch ((int) key.long1) {
            case RawBits.BYTE -> (c, k, i) -> c.putByte((byte) k.long0Array[i]);
            case RawBits.SHORT -> (c, k, i) -> c.putShort((short) k.long0Array[i]);
            case RawBits.INT, RawBits.FLOAT -> (c, k, i) -> c.putInt((int) k.long0Array[i]);
            case RawBits.LONG, RawBits.DOUBLE -> (c, k, i) -> c.putLong(k.long0Array[i]);
            default -> throw new IllegalArgumentException("Unknown number type " + key.long1);
        };
    }

    @Override
    boolean readValue(PageCursor cursor, int size, GenericKey<?> into) {
        into.long1 = cursor.getByte(); // number type, like: byte, int, short a.s.o.
        ValueWriter.ArrayType numberType = numberArrayTypeOf((byte) into.long1);
        if (numberType == null) {
            setCursorException(cursor, "non-valid number type for array, " + into.long1);
            return false;
        }
        return readArray(cursor, numberType, numberArrayElementReader(into), into);
    }

    @Override
    void initializeAsLowest(GenericKey<?> state) {
        state.initializeArrayMeta(0);
        initializeArray(state, 0, ValueWriter.ArrayType.BYTE);
    }

    @Override
    void initializeAsHighest(GenericKey<?> state) {
        state.initializeArrayMeta(0);
        initializeArray(state, 0, ValueWriter.ArrayType.BYTE);
        state.isHighestArray = true;
    }

    private static ValueWriter.ArrayType numberArrayTypeOf(byte numberType) {
        return switch (numberType) {
            case RawBits.BYTE -> ValueWriter.ArrayType.BYTE;
            case RawBits.SHORT -> ValueWriter.ArrayType.SHORT;
            case RawBits.INT -> ValueWriter.ArrayType.INT;
            case RawBits.LONG -> ValueWriter.ArrayType.LONG;
            case RawBits.FLOAT -> ValueWriter.ArrayType.FLOAT;
            case RawBits.DOUBLE -> ValueWriter.ArrayType.DOUBLE;
                // bad read, hopefully
            default -> null;
        };
    }

    private static ArrayElementReader numberArrayElementReader(GenericKey<?> key) {
        switch ((int) key.long1) {
            case RawBits.BYTE:
                return (c, into) -> {
                    key.writeInteger(c.getByte());
                    return true;
                };
            case RawBits.SHORT:
                return (c, into) -> {
                    key.writeInteger(c.getShort());
                    return true;
                };
            case RawBits.INT:
                return (c, into) -> {
                    key.writeInteger(c.getInt());
                    return true;
                };
            case RawBits.LONG:
                return (c, into) -> {
                    key.writeInteger(c.getLong());
                    return true;
                };
            case RawBits.FLOAT:
                return (c, into) -> {
                    key.writeFloatingPoint(Float.intBitsToFloat(c.getInt()));
                    return true;
                };
            case RawBits.DOUBLE:
                return (c, into) -> {
                    key.writeFloatingPoint(Double.longBitsToDouble(c.getLong()));
                    return true;
                };
            default:
                throw new IllegalArgumentException("Unknown number type " + key.long1);
        }
    }

    static void write(GenericKey<?> state, int offset, long value) {
        state.long0Array[offset] = value;
    }

    @Override
    protected void addTypeSpecificDetails(StringJoiner joiner, GenericKey<?> state) {
        joiner.add("long1=" + state.long1);
        joiner.add("long0Array=" + Arrays.toString(state.long0Array));
        super.addTypeSpecificDetails(joiner, state);
    }
}
