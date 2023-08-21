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

import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_BYTE;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_DOUBLE;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_FLOAT;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_INT;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_LONG;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_SHORT;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_TYPE;

import java.util.StringJoiner;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

class NumberType extends Type {
    // Affected key state:
    // long0 (value)
    // long1 (number type)

    NumberType(byte typeId) {
        super(ValueGroup.NUMBER, typeId, Values.of(Double.NEGATIVE_INFINITY), Values.of(Double.NaN));
    }

    @Override
    int valueSize(GenericKey<?> state) {
        return numberKeySize(state.long1) + SIZE_NUMBER_TYPE;
    }

    @Override
    void copyValue(GenericKey<?> to, GenericKey<?> from) {
        to.long0 = from.long0;
        to.long1 = from.long1;
    }

    @Override
    Value asValue(GenericKey<?> state) {
        return asValue(state.long0, state.long1);
    }

    @Override
    int compareValue(GenericKey<?> left, GenericKey<?> right) {
        return compare(
                left.long0, left.long1,
                right.long0, right.long1);
    }

    @Override
    void putValue(PageCursor cursor, GenericKey<?> state) {
        cursor.putByte((byte) state.long1);
        switch ((int) state.long1) {
            case RawBits.BYTE -> cursor.putByte((byte) state.long0);
            case RawBits.SHORT -> cursor.putShort((short) state.long0);
            case RawBits.INT, RawBits.FLOAT -> cursor.putInt((int) state.long0);
            case RawBits.LONG, RawBits.DOUBLE -> cursor.putLong(state.long0);
            default -> throw new IllegalArgumentException("Unknown number type " + state.long1);
        }
    }

    @Override
    boolean readValue(PageCursor cursor, int size, GenericKey<?> into) {
        into.long1 = cursor.getByte();
        switch ((int) into.long1) {
            case RawBits.BYTE:
                into.long0 = cursor.getByte();
                return true;
            case RawBits.SHORT:
                into.long0 = cursor.getShort();
                return true;
            case RawBits.INT:
            case RawBits.FLOAT:
                into.long0 = cursor.getInt();
                return true;
            case RawBits.LONG:
            case RawBits.DOUBLE:
                into.long0 = cursor.getLong();
                return true;
            default:
                return false;
        }
    }

    static int numberKeySize(long long1) {
        return switch ((int) long1) {
            case RawBits.BYTE -> SIZE_NUMBER_BYTE;
            case RawBits.SHORT -> SIZE_NUMBER_SHORT;
            case RawBits.INT -> SIZE_NUMBER_INT;
            case RawBits.LONG -> SIZE_NUMBER_LONG;
            case RawBits.FLOAT -> SIZE_NUMBER_FLOAT;
            case RawBits.DOUBLE -> SIZE_NUMBER_DOUBLE;
            default -> throw new IllegalArgumentException("Unknown number type " + long1);
        };
    }

    static NumberValue asValue(long long0, long long1) {
        return RawBits.asNumberValue(long0, (byte) long1);
    }

    static int compare(long this_long0, long this_long1, long that_long0, long that_long1) {
        return RawBits.compare(this_long0, (byte) this_long1, that_long0, (byte) that_long1);
    }

    static void write(GenericKey<?> state, long value, byte numberType) {
        state.long0 = value;
        state.long1 = numberType;
    }

    @Override
    protected void addTypeSpecificDetails(StringJoiner joiner, GenericKey<?> state) {
        joiner.add("long0=" + state.long0);
        joiner.add("long1=" + state.long1);
    }
}
