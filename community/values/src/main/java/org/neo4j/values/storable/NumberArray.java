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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import org.neo4j.graphdb.spatial.Geometry;

public abstract class NumberArray extends ArrayValue {
    abstract int compareTo(IntegralArray other);

    abstract int compareTo(FloatingPointArray other);

    @Override
    public abstract NumberValue value(int offset);

    @Override
    protected int unsafeCompareTo(Value otherValue) {
        if (otherValue instanceof IntegralArray) {
            return compareTo((IntegralArray) otherValue);
        } else if (otherValue instanceof FloatingPointArray) {
            return compareTo((FloatingPointArray) otherValue);
        } else {
            throw new IllegalArgumentException("Cannot compare different values");
        }
    }

    @Override
    public final boolean equals(boolean[] x) {
        return false;
    }

    @Override
    public final boolean equals(char[] x) {
        return false;
    }

    @Override
    public final boolean equals(String[] x) {
        return false;
    }

    @Override
    public final boolean equals(Geometry[] x) {
        return false;
    }

    @Override
    public final boolean equals(ZonedDateTime[] x) {
        return false;
    }

    @Override
    public final boolean equals(LocalDate[] x) {
        return false;
    }

    @Override
    public final boolean equals(DurationValue[] x) {
        return false;
    }

    @Override
    public final boolean equals(LocalDateTime[] x) {
        return false;
    }

    @Override
    public final boolean equals(LocalTime[] x) {
        return false;
    }

    @Override
    public final boolean equals(OffsetTime[] x) {
        return false;
    }
}
