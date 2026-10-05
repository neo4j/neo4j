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
package org.neo4j.cypher.operations;

import static java.lang.String.format;
import static org.neo4j.exceptions.ArithmeticException.numericValueOutOfRange;
import static org.neo4j.values.storable.VectorValue.MAX_VECTOR_DIMENSIONS;
import static org.neo4j.values.storable.VectorValue.MIN_VECTOR_DIMENSIONS;

import java.util.List;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.Float16Format;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

final class VectorUtils {
    private VectorUtils() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    static VectorValue int8Vector(SequenceValue vectorSequence) {
        if (vectorSequence instanceof ByteArray bytes) {
            return Values.int8Vector(bytes.asObjectCopy());
        } else {
            int index = 0;
            int length = vectorSequence.intSize();
            byte[] values = new byte[length];
            for (AnyValue value : vectorSequence) {
                if (value instanceof NumberValue number) {
                    values[index++] = safeCastToByte(number.longValue());
                } else {
                    throw invalidVectorType(value);
                }
            }
            return Values.int8Vector(values);
        }
    }

    static VectorValue int16Vector(SequenceValue vectorSequence) {
        if (vectorSequence instanceof ShortArray shorts) {
            return Values.int16Vector(shorts.asObjectCopy());
        } else {
            int index = 0;
            int length = vectorSequence.intSize();
            short[] values = new short[length];
            for (AnyValue value : vectorSequence) {
                if (value instanceof NumberValue number) {
                    values[index++] = safeCastToShort(number.longValue());
                } else {
                    throw invalidVectorType(value);
                }
            }
            return Values.int16Vector(values);
        }
    }

    static VectorValue int32Vector(SequenceValue vectorSequence) {
        if (vectorSequence instanceof IntArray ints) {
            return Values.int32Vector(ints.asObjectCopy());
        } else {
            int index = 0;
            int length = vectorSequence.intSize();
            int[] values = new int[length];
            for (AnyValue value : vectorSequence) {
                if (value instanceof NumberValue number) {
                    values[index++] = safeCastToInt(number.longValue());
                } else {
                    throw invalidVectorType(value);
                }
            }
            return Values.int32Vector(values);
        }
    }

    static VectorValue int64Vector(SequenceValue vectorSequence) {
        if (vectorSequence instanceof LongArray longs) {
            return Values.int64Vector(longs.asObjectCopy());
        } else {
            int index = 0;
            int length = vectorSequence.intSize();
            long[] values = new long[length];
            for (AnyValue value : vectorSequence) {
                if (value instanceof FloatingPointValue fp) {
                    values[index++] = safeCastToLong(fp.doubleValue());
                } else if (value instanceof NumberValue number) {
                    values[index++] = number.longValue();
                } else {
                    throw invalidVectorType(value);
                }
            }
            return Values.int64Vector(values);
        }
    }

    static VectorValue float32Vector(SequenceValue vectorSequence) {
        // NOTE that even if the incoming vectorSequence is an instance of FloatArray
        //     we still need to verify the contents is finite so there is no point of
        //     short-circuiting here.
        int index = 0;
        int length = vectorSequence.intSize();
        float[] values = new float[length];
        for (AnyValue value : vectorSequence) {
            if (value instanceof NumberValue numberValue) {
                values[index++] = numberValue.floatValue();
            } else {
                throw invalidVectorType(value);
            }
        }
        return Values.float32Vector(values);
    }

    static VectorValue float16Vector(SequenceValue vectorSequence) {
        int index = 0;
        int length = vectorSequence.intSize();
        short[] values = new short[length];
        for (AnyValue value : vectorSequence) {
            if (value instanceof NumberValue numberValue) {
                values[index++] = Float16Format.FLOAT16.toFloat16(numberValue.floatValue());
            } else {
                throw invalidVectorType(value);
            }
        }
        return Values.float16Vector(Float16Format.FLOAT16, values);
    }

    static VectorValue bfloat16Vector(SequenceValue vectorSequence) {
        int index = 0;
        int length = vectorSequence.intSize();
        short[] values = new short[length];
        for (AnyValue value : vectorSequence) {
            if (value instanceof NumberValue numberValue) {
                values[index++] = Float16Format.BFLOAT16.toFloat16(numberValue.floatValue());
            } else {
                throw invalidVectorType(value);
            }
        }
        return Values.float16Vector(Float16Format.BFLOAT16, values);
    }

    static VectorValue float64Vector(SequenceValue vectorSequence) {
        // NOTE that even if the incoming vectorSequence is an instance of DoubleArray
        //     we still need to verify the contents is finite so there is no point of
        //     short-circuiting here.
        int index = 0;
        int length = vectorSequence.intSize();
        double[] values = new double[length];
        for (AnyValue value : vectorSequence) {
            if (value instanceof NumberValue numberValue) {
                values[index++] = numberValue.doubleValue();
            } else {
                throw invalidVectorType(value);
            }
        }
        return Values.float64Vector(values);
    }

    static byte safeCastToByte(long value) {
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw numericValueOutOfRange(String.valueOf(value), "vector()");
        }
        return (byte) value;
    }

    static short safeCastToShort(long value) {
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw numericValueOutOfRange(String.valueOf(value), "vector()");
        }
        return (short) value;
    }

    static int safeCastToInt(long value) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw numericValueOutOfRange(String.valueOf(value), "vector()");
        }
        return (int) value;
    }

    static long safeCastToLong(double value) {
        if (value < Long.MIN_VALUE || value > Long.MAX_VALUE) {
            throw numericValueOutOfRange(String.valueOf(value), "vector()");
        }
        return (long) value;
    }

    static Value assertDimension(VectorValue vectorValue, AnyValue dimension) {
        long dimensionValue = getDimension(dimension);
        if (vectorValue.dimensions() != dimensionValue) {
            throw CypherTypeException.wrongVectorDimension(
                    vectorValue.prettyPrint(), vectorValue.nestedTypeName(), dimensionValue, vectorValue.dimensions());
        }
        return vectorValue;
    }

    static long getDimension(AnyValue dimension) {
        if (!(dimension instanceof IntegralValue dimensionValue)) {
            throw CypherTypeException.functionArgumentWrongType(
                    format("Invalid input for function 'vector()': Expected an integer but got %s", dimension),
                    "vector",
                    dimension.toString(),
                    List.of("INTEGER"),
                    dimension.getTypeName());
        }

        if (dimensionValue.longValue() < MIN_VECTOR_DIMENSIONS || dimensionValue.longValue() > MAX_VECTOR_DIMENSIONS) {
            throw InvalidArgumentException.argumentOutOfRange(
                    "vector", "dimension", MIN_VECTOR_DIMENSIONS, MAX_VECTOR_DIMENSIONS, dimensionValue.longValue());
        }

        return dimensionValue.longValue();
    }

    static CypherTypeException invalidVectorType(AnyValue value) {
        return CypherTypeException.functionArgumentWrongType(
                "Invalid input for function 'vector()': Expected a NUMBER, got: " + value,
                "vector",
                value.prettyPrint(),
                List.of("INTEGER", "FLOAT"),
                CypherTypeValueMapper.valueType(value));
    }

    static CypherTypeException invalidVector(AnyValue vector) {
        return CypherTypeException.functionArgumentWrongType(
                format("Invalid input for function 'vector()': Expected a string or list but got %s", vector),
                "vector",
                vector.toString(),
                List.of("STRING", "LIST<INTEGER | FLOAT>"),
                vector.getTypeName());
    }
}
