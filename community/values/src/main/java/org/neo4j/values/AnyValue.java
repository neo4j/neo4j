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
package org.neo4j.values;

import org.neo4j.memory.Measurable;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;

public abstract class AnyValue implements Measurable {
    // this should be final, but Mockito barfs if it is,
    // so we need to just manually ensure it isn't overridden
    @Override
    public boolean equals(Object other) {
        return internalEquals(other);
    }

    // In Cypher RETURN null = null; returns null. Therefore, in a binary equals we
    // sometimes need to return false when matching e.g CASE null WHEN null THEN... shouldn't match on null
    public boolean equalsWithNoValueCheck(Object other) {
        if (this == Values.NO_VALUE && other == Values.NO_VALUE) return false;
        return internalEquals(other);
    }

    @Override
    public final int hashCode() {
        return computeHash();
    }

    protected abstract boolean equalTo(Object other);

    protected abstract int computeHash();

    public abstract <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E;

    public boolean isSequenceValue() {
        return false; // per default Values are no SequenceValues
    }

    public boolean isIncomparableType() {
        return false;
    }

    public abstract Equality ternaryEquals(AnyValue other);

    public abstract <T> T map(ValueMapper<T> mapper);

    public abstract String getTypeName();

    public abstract ValueRepresentation valueRepresentation();

    /**
     * @return {@code true} if at least one operand is NaN and the other is a number
     */
    public static boolean isNanAndNumber(AnyValue value1, AnyValue value2) {
        return (value1 instanceof FloatingPointValue
                        && ((FloatingPointValue) value1).isNaN()
                        && value2 instanceof NumberValue)
                || (value2 instanceof FloatingPointValue
                        && ((FloatingPointValue) value2).isNaN()
                        && value1 instanceof NumberValue);
    }

    /**
     * @return {@code true} if at least one operand is NaN
     */
    public static boolean hasNaNOperand(AnyValue value1, AnyValue value2) {
        return isNaN(value1) || isNaN(value2);
    }

    public static boolean isNaN(AnyValue value) {
        return value instanceof FloatingPointValue && ((FloatingPointValue) value).isNaN();
    }

    protected boolean internalEquals(Object other) {
        return this == other || other != null && equalTo(other);
    }
}
