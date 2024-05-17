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
package org.neo4j.values.storable;

import org.neo4j.values.AnyValue;

public abstract class NumberValue extends ScalarValue {
    static long castToLong(String name, AnyValue value) {
        if (value == null) {
            return 0L;
        }

        if (value instanceof final IntegralValue integralValue) {
            return integralValue.longValue();
        }

        throw new IllegalArgumentException(name + " must be an integral value, but was a "
                + value.getClass().getSimpleName());
    }

    static double safeCastFloatingPoint(String name, AnyValue value, double defaultValue) {
        if (value == null) {
            return defaultValue;
        }

        if (!(value instanceof final NumberValue numberValue)) {
            throw new IllegalArgumentException(name + " must be a number value, but was a "
                    + value.getClass().getSimpleName());
        }

        return numberValue.doubleValue();
    }

    public abstract long longValue();

    public abstract float floatValue();

    public abstract double doubleValue();

    public abstract int compareTo(IntegralValue other);

    public abstract int compareTo(FloatingPointValue other);

    @Override
    protected int unsafeCompareTo(Value otherValue) {
        if (otherValue instanceof final IntegralValue integralValue) {
            return compareTo(integralValue);
        }
        if (otherValue instanceof final FloatingPointValue floatingPointValue) {
            return compareTo(floatingPointValue);
        }
        throw new IllegalArgumentException("Cannot compare different values");
    }

    @Override
    public abstract Number asObjectCopy();

    @Override
    public Number asObject() {
        return asObjectCopy();
    }

    @Override
    public final boolean equals(boolean x) {
        return false;
    }

    @Override
    public final boolean equals(char x) {
        return false;
    }

    @Override
    public final boolean equals(String x) {
        return false;
    }

    public abstract NumberValue minus(long b);

    public abstract NumberValue minus(double b);

    public abstract NumberValue plus(long b);

    public abstract NumberValue plus(double b);

    public abstract NumberValue times(long b);

    public abstract NumberValue times(double b);

    public abstract NumberValue dividedBy(long b);

    public abstract FloatingPointValue dividedBy(double b);

    public NumberValue minus(NumberValue numberValue) {
        if (numberValue instanceof IntegralValue) {
            return minus(numberValue.longValue());
        }
        if (numberValue instanceof FloatingPointValue) {
            return minus(numberValue.doubleValue());
        }
        throw new IllegalArgumentException("Cannot subtract " + numberValue);
    }

    public NumberValue plus(NumberValue numberValue) {
        if (numberValue instanceof IntegralValue) {
            return plus(numberValue.longValue());
        }
        if (numberValue instanceof FloatingPointValue) {
            return plus(numberValue.doubleValue());
        }
        throw new IllegalArgumentException("Cannot subtract " + numberValue);
    }

    public NumberValue times(NumberValue numberValue) {
        if (numberValue instanceof IntegralValue) {
            return times(numberValue.longValue());
        }
        if (numberValue instanceof FloatingPointValue) {
            return times(numberValue.doubleValue());
        }
        throw new IllegalArgumentException("Cannot multiply with " + numberValue);
    }

    public NumberValue divideBy(NumberValue numberValue) {
        if (numberValue instanceof IntegralValue) {
            return dividedBy(numberValue.longValue());
        }
        if (numberValue instanceof FloatingPointValue) {
            return dividedBy(numberValue.doubleValue());
        }
        throw new IllegalArgumentException("Cannot divide by " + numberValue);
    }
}
