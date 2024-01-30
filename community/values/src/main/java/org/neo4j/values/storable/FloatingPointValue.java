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

import org.neo4j.hashing.HashFunction;
import org.neo4j.values.utils.ValueMath;

public abstract class FloatingPointValue extends NumberValue {
    @Override
    public long longValue() {
        return (long) doubleValue();
    }

    @Override
    public final boolean equals(long x) {
        return NumberValues.numbersEqual(doubleValue(), x);
    }

    @Override
    public final boolean equals(double x) {
        return doubleValue() == x;
    }

    @Override
    protected final int computeHash() {
        return NumberValues.hash(doubleValue());
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        return hashFunction.update(hash, Double.doubleToLongBits(doubleValue()));
    }

    @Override
    public boolean equalTo(Object other) {
        return other instanceof Value && equals((Value) other);
    }

    @Override
    public final boolean equals(Value other) {
        if (other instanceof FloatingPointValue that) {
            return this.doubleValue() == that.doubleValue();
        } else if (other instanceof IntegralValue that) {
            return NumberValues.numbersEqual(this.doubleValue(), that.longValue());
        } else {
            return false;
        }
    }

    @Override
    public NumberType numberType() {
        return NumberType.FLOATING_POINT;
    }

    @Override
    public int compareTo(IntegralValue other) {
        return NumberValues.compareDoubleAgainstLong(doubleValue(), other.longValue());
    }

    @Override
    public int compareTo(FloatingPointValue other) {
        return Double.compare(doubleValue(), other.doubleValue());
    }

    public boolean isNaN() {
        return Double.isNaN(this.doubleValue());
    }

    @Override
    boolean ternaryUndefined() {
        return isNaN();
    }

    @Override
    public DoubleValue minus(long b) {
        return ValueMath.subtract(doubleValue(), b);
    }

    @Override
    public DoubleValue minus(double b) {
        return ValueMath.subtract(doubleValue(), b);
    }

    @Override
    public DoubleValue plus(long b) {
        return ValueMath.add(doubleValue(), b);
    }

    @Override
    public DoubleValue plus(double b) {
        return ValueMath.add(doubleValue(), b);
    }

    @Override
    public DoubleValue times(long b) {
        return ValueMath.multiply(doubleValue(), b);
    }

    @Override
    public DoubleValue times(double b) {
        return ValueMath.multiply(doubleValue(), b);
    }

    @Override
    public DoubleValue dividedBy(long b) {
        return Values.doubleValue(doubleValue() / b);
    }

    @Override
    public DoubleValue dividedBy(double b) {
        return Values.doubleValue(doubleValue() / b);
    }
}
