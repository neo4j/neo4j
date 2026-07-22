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

import static java.lang.Math.toIntExact;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.StringJoiner;
import org.neo4j.graphdb.Vector.CoordinateType;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Float64Vector;
import org.neo4j.values.storable.Int16Vector;
import org.neo4j.values.storable.Int32Vector;
import org.neo4j.values.storable.Int64Vector;
import org.neo4j.values.storable.Int8Vector;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

/** The vector key represents vectors for Range indexes.
 * <p />
 *  The type encodes its coordinate type using the permitted subclasses which are instantiated in
 *  {@link org.neo4j.kernel.impl.index.schema.Types}.
 *
 *  When a value is written to a key, we extract the dimension and encode the coordinates to a byte-representation
 *  using ByteBuffer. This representation will by default be big-endian (ByteBuffer default).
 *  <p/>
 *  When keys are serialized, the dimension will be encoded in {@link org.neo4j.io.pagecache.PageCursor}-defined endianness,
 *  whereas the data will be written as-is (i.e. big endian) by performing a memory copy.
 */
public abstract sealed class VectorKeyType extends Type
        permits VectorKeyType.Int8VectorKey,
                VectorKeyType.Int16VectorKey,
                VectorKeyType.Int32VectorKey,
                VectorKeyType.Int64VectorKey,
                VectorKeyType.Float32VectorKey,
                VectorKeyType.Float64VectorKey {

    // Affected key state:
    // long0 (dimension)
    // byteArray (data)

    // Size of the inner elements
    final int elementSize;

    abstract int arrayCompare(byte[] left, byte[] right, int numBytes);

    private static ValueGroup fromCoordinateType(CoordinateType coordinateType) {
        /* Note: This ensures that we represent all CoordinateTypes. */
        return switch (coordinateType) {
            case INTEGER8 -> ValueGroup.INT8_VECTOR;
            case INTEGER16 -> ValueGroup.INT16_VECTOR;
            case INTEGER32 -> ValueGroup.INT32_VECTOR;
            case INTEGER64 -> ValueGroup.INT64_VECTOR;
            case FLOAT32 -> ValueGroup.FLOAT32_VECTOR;
            case FLOAT64 -> ValueGroup.FLOAT64_VECTOR;
        };
    }

    private VectorKeyType(CoordinateType coordinateType, byte typeId, int elementSize) {
        super(fromCoordinateType(coordinateType), typeId, null, null);
        this.elementSize = elementSize;
    }

    static final class Int8VectorKey extends VectorKeyType {
        Int8VectorKey(byte typeId) {
            super(CoordinateType.INTEGER8, typeId, Byte.BYTES);
        }

        public void write(GenericKey<?> state, byte[] values) {
            ByteBuffer bb = prepareStateAndGetWriteBuffer(state, values.length);
            bb.put(values);
        }

        @Override
        int arrayCompare(byte[] left, byte[] right, int numBytes) {
            return compareBytes(left, right, numBytes);
        }

        static int compareBytes(byte[] left, byte[] right, int numBytes) {
            return Arrays.compare(left, 0, numBytes, right, 0, numBytes);
        }

        @Override
        public Value asValue(GenericKey<?> state) {
            return asValue(dimension(state), state.byteArray);
        }

        static Int8Vector asValue(int dimensions, byte[] data) {
            ByteBuffer bb = ByteBuffer.wrap(data);
            byte[] copy = new byte[dimensions];
            bb.get(copy, 0, copy.length);
            return Values.int8Vector(copy);
        }
    }

    static final class Int16VectorKey extends VectorKeyType {
        Int16VectorKey(byte typeId) {
            super(CoordinateType.INTEGER16, typeId, Short.BYTES);
        }

        void write(GenericKey<?> state, short[] values) {
            ByteBuffer bb = prepareStateAndGetWriteBuffer(state, values.length);
            bb.asShortBuffer().put(values);
        }

        @Override
        int arrayCompare(byte[] l, byte[] r, int numBytes) {
            return compareBytes(l, r, numBytes);
        }

        static int compareBytes(byte[] left, byte[] right, int numBytes) {
            ShortBuffer lb = ByteBuffer.wrap(left, 0, numBytes).asShortBuffer();
            ShortBuffer rb = ByteBuffer.wrap(right, 0, numBytes).asShortBuffer();
            return lb.compareTo(rb);
        }

        @Override
        public Value asValue(GenericKey<?> state) {
            return asValue(dimension(state), state.byteArray);
        }

        static Int16Vector asValue(int dimensions, byte[] data) {
            ShortBuffer bb = ByteBuffer.wrap(data).asShortBuffer();
            short[] copy = new short[dimensions];
            bb.get(copy, 0, copy.length);
            return Values.int16Vector(copy);
        }
    }

    static final class Int32VectorKey extends VectorKeyType {
        Int32VectorKey(byte typeId) {
            super(CoordinateType.INTEGER32, typeId, Integer.BYTES);
        }

        void write(GenericKey<?> state, int[] values) {
            ByteBuffer bb = prepareStateAndGetWriteBuffer(state, values.length);
            bb.asIntBuffer().put(values);
        }

        @Override
        int arrayCompare(byte[] l, byte[] r, int numBytes) {
            return compareBytes(l, r, numBytes);
        }

        static int compareBytes(byte[] left, byte[] right, int numBytes) {
            IntBuffer lb = ByteBuffer.wrap(left, 0, numBytes).asIntBuffer();
            IntBuffer rb = ByteBuffer.wrap(right, 0, numBytes).asIntBuffer();
            return lb.compareTo(rb);
        }

        @Override
        public Value asValue(GenericKey<?> state) {
            return asValue(dimension(state), state.byteArray);
        }

        static Int32Vector asValue(int dimensions, byte[] data) {
            IntBuffer bb = ByteBuffer.wrap(data).asIntBuffer();
            int[] copy = new int[dimensions];
            bb.get(copy, 0, copy.length);
            return Values.int32Vector(copy);
        }
    }

    static final class Int64VectorKey extends VectorKeyType {
        Int64VectorKey(byte typeId) {
            super(CoordinateType.INTEGER64, typeId, Long.BYTES);
        }

        void write(GenericKey<?> state, long[] values) {
            ByteBuffer bb = prepareStateAndGetWriteBuffer(state, values.length);
            bb.asLongBuffer().put(values);
        }

        @Override
        int arrayCompare(byte[] l, byte[] r, int numBytes) {
            return compareBytes(l, r, numBytes);
        }

        static int compareBytes(byte[] left, byte[] right, int numBytes) {
            LongBuffer lb = ByteBuffer.wrap(left, 0, numBytes).asLongBuffer();
            LongBuffer rb = ByteBuffer.wrap(right, 0, numBytes).asLongBuffer();
            return lb.compareTo(rb);
        }

        @Override
        public Value asValue(GenericKey<?> state) {
            return asValue(dimension(state), state.byteArray);
        }

        static Int64Vector asValue(int dimensions, byte[] data) {
            LongBuffer bb = ByteBuffer.wrap(data).asLongBuffer();
            long[] copy = new long[dimensions];
            bb.get(copy, 0, copy.length);
            return Values.int64Vector(copy);
        }
    }

    static final class Float32VectorKey extends VectorKeyType {
        Float32VectorKey(byte typeId) {
            super(CoordinateType.FLOAT32, typeId, Float.BYTES);
        }

        @Override
        int arrayCompare(byte[] l, byte[] r, int numBytes) {
            return compareBytes(l, r, numBytes);
        }

        static int compareBytes(byte[] left, byte[] right, int numBytes) {
            FloatBuffer lb = ByteBuffer.wrap(left, 0, numBytes).asFloatBuffer();
            FloatBuffer rb = ByteBuffer.wrap(right, 0, numBytes).asFloatBuffer();
            return lb.compareTo(rb);
        }

        void write(GenericKey<?> state, float[] values) {
            ByteBuffer bb = prepareStateAndGetWriteBuffer(state, values.length);
            bb.asFloatBuffer().put(values);
        }

        @Override
        public Value asValue(GenericKey<?> state) {
            return asValue(dimension(state), state.byteArray);
        }

        static Float32Vector asValue(int dimensions, byte[] data) {
            FloatBuffer bb = ByteBuffer.wrap(data).asFloatBuffer();
            float[] copy = new float[dimensions];
            bb.get(copy, 0, copy.length);
            return Values.float32Vector(copy);
        }
    }

    static final class Float64VectorKey extends VectorKeyType {
        Float64VectorKey(byte typeId) {
            super(CoordinateType.FLOAT64, typeId, Double.BYTES);
        }

        @Override
        int arrayCompare(byte[] l, byte[] r, int numBytes) {
            return compareBytes(l, r, numBytes);
        }

        static int compareBytes(byte[] left, byte[] right, int numBytes) {
            DoubleBuffer lb = ByteBuffer.wrap(left, 0, numBytes).asDoubleBuffer();
            DoubleBuffer rb = ByteBuffer.wrap(right, 0, numBytes).asDoubleBuffer();
            return lb.compareTo(rb);
        }

        void write(GenericKey<?> state, double[] values) {
            ByteBuffer bb = prepareStateAndGetWriteBuffer(state, values.length);
            bb.asDoubleBuffer().put(values);
        }

        @Override
        public Value asValue(GenericKey<?> state) {
            return asValue(dimension(state), state.byteArray);
        }

        static Float64Vector asValue(int dimensions, byte[] data) {
            DoubleBuffer bb = ByteBuffer.wrap(data).asDoubleBuffer();
            double[] copy = new double[dimensions];
            bb.get(copy, 0, copy.length);
            return Values.float64Vector(copy);
        }
    }

    @Override
    int valueSize(GenericKey<?> state) {
        if (isExtremeValue(state)) {
            /* The extreme values are virtual */
            return 0;
        }
        return Integer.BYTES /* dimension header */ + elementSize * dimension(state) /* data */;
    }

    private static void setInfinum(GenericKey<?> state) {
        state.long0 = VectorValue.MIN_VECTOR_DIMENSIONS - 1;
    }

    private static void setSuprenum(GenericKey<?> state) {
        state.long0 = VectorValue.MAX_VECTOR_DIMENSIONS + 1;
    }

    static boolean isExtremeValue(GenericKey<?> state) {
        return state.long0 == (VectorValue.MIN_VECTOR_DIMENSIONS - 1)
                || state.long0 == (VectorValue.MAX_VECTOR_DIMENSIONS + 1);
    }

    private static int dimension(GenericKey<?> state) {
        assert isExtremeValue(state)
                        || (state.long0 >= VectorValue.MIN_VECTOR_DIMENSIONS
                                && state.long0 <= VectorValue.MAX_VECTOR_DIMENSIONS)
                : "Corrupt state, expected " + VectorValue.MIN_VECTOR_DIMENSIONS + "<= <value> <= "
                        + VectorValue.MAX_VECTOR_DIMENSIONS + " but was " + state.long0;
        return toIntExact(state.long0);
    }

    @Override
    void copyValue(GenericKey<?> to, GenericKey<?> from) {
        to.long0 = from.long0;
        if (!isExtremeValue(from)) {
            int numBytes = elementSize * dimension(from);
            to.byteArray = ensureBigEnough(to.byteArray, numBytes);
            System.arraycopy(from.byteArray, 0, to.byteArray, 0, numBytes);
        }
    }

    @Override
    String toString(GenericKey<?> state) {
        /* Don't deserialize an entire object for the string representation */
        return "VectorKeyType<%s, %s>".formatted(valueGroup, dimension(state));
    }

    @Override
    protected void addTypeSpecificDetails(StringJoiner joiner, GenericKey<?> state) {
        // Just used for tests
        StringBuilder arr = new StringBuilder();
        int numBytes = elementSize * dimension(state);
        arr.append("data=[");
        arr.append(state.byteArray[0]);
        for (int i = 1; i < numBytes; i++) {
            arr.append(", ");
            arr.append(state.byteArray[i]);
        }
        arr.append("]");
        joiner.add(arr.toString());
    }

    @Override
    int compareValue(GenericKey<?> left, GenericKey<?> right) {
        assert left.type == right.type;

        // TODO: Determine if useful...
        /* TextKey hints that the keys may be reused, so then it makes sense with a shortcut
        to check if we compare with the identity. */
        if (left == right) {
            return 0;
        }

        /* Vector comparison states that we order the keys first by dimension, */
        int comparison = Long.compare(left.long0, right.long0);
        if (comparison != 0) {
            return comparison;
        }

        if (isExtremeValue(left)) {
            /* Intermission: If we handle extreme values, then we should let the Inclusion in the
            super class deal with the comparison. */
            return 0;
        }

        /* ... and then the pairwise component order (lexicographical sorting). */
        return arrayCompare(left.byteArray, right.byteArray, elementSize * dimension(left));
    }

    @Override
    void initializeAsLowest(GenericKey<?> state) {
        setInfinum(state);
    }

    @Override
    void initializeAsHighest(GenericKey<?> state) {
        setSuprenum(state);
    }

    @Override
    void putValue(PageCursor cursor, GenericKey<?> state) {
        assert !isExtremeValue(state) : "You are not allowed to serialize extreme values";
        int numBytes = elementSize * dimension(state);
        cursor.putInt((int) state.long0);
        cursor.putBytes(state.byteArray, 0, numBytes);
    }

    @Override
    boolean readValue(PageCursor cursor, int size, GenericKey<?> into) {
        into.long0 = cursor.getInt();
        if (into.long0 < VectorValue.MIN_VECTOR_DIMENSIONS || into.long0 > VectorValue.MAX_VECTOR_DIMENSIONS) {
            cursor.setCursorException("Corrupt state, expected " + VectorValue.MIN_VECTOR_DIMENSIONS + "<= <value> <= "
                    + VectorValue.MAX_VECTOR_DIMENSIONS + " but was " + into.long0);
            return false;
        }
        size -= Integer.BYTES;

        int expectedNumBytes = elementSize * dimension(into);
        if (size < expectedNumBytes) {
            cursor.setCursorException(
                    "Corrupt state, expected %d bytes remaining, but was %d".formatted(expectedNumBytes, size));
            return false;
        }

        into.byteArray = ensureBigEnough(into.byteArray, expectedNumBytes);
        cursor.getBytes(into.byteArray, 0, expectedNumBytes);
        return true;
    }

    <KEY extends GenericKey<KEY>> ByteBuffer prepareStateAndGetWriteBuffer(GenericKey<KEY> state, int dimensions) {
        state.long0 = dimensions;
        state.byteArray = ensureBigEnough(state.byteArray, elementSize * dimensions);
        return ByteBuffer.wrap(state.byteArray, 0, elementSize * dimensions);
    }
}
