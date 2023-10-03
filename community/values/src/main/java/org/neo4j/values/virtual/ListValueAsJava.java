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
package org.neo4j.values.virtual;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.NonPrimitiveArray;
import org.neo4j.values.storable.NumberArray;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.StringArray;

public class ListValueAsJava {
    private ListValueAsJava() {}

    /**
     * Returns a java object representation of the specified ArrayValueListValue
     * without copying values.
     * Or null if no such object is implemented.
     */
    public static List<?> asObject(ListValue.ArrayValueListValue value) {
        final var array = value.toStorableArray();

        if (array instanceof NumberArray numberArray) {
            return numberArrayAsObject(numberArray);
        } else if (array instanceof StringArray stringArray) {
            return Arrays.asList(stringArray.asObject());
        } else if (array instanceof BooleanArray booleanArray) {
            return new BooleanArrayList(booleanArray.asObject());
        } else if (array instanceof NonPrimitiveArray<?> nonPrimitiveArray) {
            return Arrays.asList(nonPrimitiveArray.asObject());
        } else {
            return null;
        }
    }

    private static List<?> numberArrayAsObject(final NumberArray array) {
        if (array instanceof DoubleArray doubleArray) {
            return new DoubleArrayNumberList(doubleArray.asObject());
        } else if (array instanceof FloatArray floatArray) {
            return new FloatArrayNumberList(floatArray.asObject());
        } else if (array instanceof IntArray intArray) {
            return new IntArrayNumberList(intArray.asObject());
        } else if (array instanceof LongArray longArray) {
            return new LongArrayNumberList(longArray.asObject());
        } else if (array instanceof ShortArray shortArray) {
            return new ShortArrayNumberList(shortArray.asObject());
        } else if (array instanceof ByteArray byteArray) {
            // Copy here because ByteArray is mutable
            return new ByteArrayNumberList(byteArray.asObjectCopy());
        } else {
            return null;
        }
    }
}

class DoubleArrayNumberList extends AbstractList<Number> {
    private final double[] array;

    DoubleArrayNumberList(double[] array) {
        this.array = array;
    }

    @Override
    public Number get(int index) {
        return array[index];
    }

    @Override
    public int size() {
        return array.length;
    }
}

class FloatArrayNumberList extends AbstractList<Number> {
    private final float[] array;

    FloatArrayNumberList(float[] array) {
        this.array = array;
    }

    @Override
    public Number get(int index) {
        return array[index];
    }

    @Override
    public int size() {
        return array.length;
    }
}

class ByteArrayNumberList extends AbstractList<Number> {
    private final byte[] array;

    ByteArrayNumberList(byte[] array) {
        this.array = array;
    }

    @Override
    public Number get(int index) {
        return array[index];
    }

    @Override
    public int size() {
        return array.length;
    }
}

class ShortArrayNumberList extends AbstractList<Number> {
    private final short[] array;

    ShortArrayNumberList(short[] array) {
        this.array = array;
    }

    @Override
    public Number get(int index) {
        return array[index];
    }

    @Override
    public int size() {
        return array.length;
    }
}

class IntArrayNumberList extends AbstractList<Number> {
    private final int[] array;

    IntArrayNumberList(int[] array) {
        this.array = array;
    }

    @Override
    public Number get(int index) {
        return array[index];
    }

    @Override
    public int size() {
        return array.length;
    }
}

class LongArrayNumberList extends AbstractList<Number> {
    private final long[] array;

    LongArrayNumberList(long[] array) {
        this.array = array;
    }

    @Override
    public Number get(int index) {
        return array[index];
    }

    @Override
    public int size() {
        return array.length;
    }
}

class BooleanArrayList extends AbstractList<Boolean> {
    private final boolean[] array;

    BooleanArrayList(boolean[] array) {
        this.array = array;
    }

    @Override
    public Boolean get(int index) {
        return array[index];
    }

    @Override
    public int size() {
        return array.length;
    }
}
