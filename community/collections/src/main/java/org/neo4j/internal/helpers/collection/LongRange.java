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
package org.neo4j.internal.helpers.collection;

import static java.lang.String.format;
import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.util.Preconditions.requireNonNegative;

import java.util.Objects;
import java.util.stream.LongStream;

public final class LongRange {
    public static final LongRange EMPTY_RANGE = new LongRange(-1, -1);

    public static LongRange range(long from, long to) {
        if (to < from) {
            return EMPTY_RANGE;
        }
        assertIsRange(from, to);
        return new LongRange(from, to);
    }

    public static LongRange join(LongRange rangeA, LongRange rangeB) {
        if (!rangeA.isAdjacent(rangeB)) {
            throw new IllegalArgumentException(
                    format("Fail to join ranges %s and %s since they do not form continuous range.", rangeA, rangeB));
        }
        return LongRange.range(rangeA.from, rangeB.to);
    }

    public static void assertIsRange(long from, long to) {
        requireNonNegative(from);
        checkArgument(
                to >= from, "Not a valid range. Range to [%d] must be higher or equal to range from [%d].", to, from);
    }

    private final long from;
    private final long to;

    private LongRange(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public boolean isAdjacent(LongRange candidate) {
        if (isEmpty()) {
            return false;
        }
        return this.to + 1 == candidate.from;
    }

    /**
     * @param val value to compare whether or not it's within this range.
     * @return {@code true} if {@code from <= val <= to}, i.e. inclusive from and inclusive to.
     */
    public boolean isWithinRange(long val) {
        if (isEmpty()) {
            return false;
        }
        return val >= from && val <= to;
    }

    /**
     * @param val value to compare whether or not it's within this range.
     * @return {@code true} if {@code from <= val < to}, i.e. inclusive from and exclusive to.
     */
    public boolean isWithinRangeExclusiveTo(long val) {
        if (isEmpty()) {
            return false;
        }
        return val >= from && val < to;
    }

    public LongStream stream() {
        return isEmpty() ? LongStream.empty() : LongStream.rangeClosed(from, to);
    }

    public boolean isEmpty() {
        return from == -1;
    }

    @Override
    public String toString() {
        return "LongRange{" + "from=" + from + ", to=" + to + '}';
    }

    public long from() {
        return from;
    }

    public long to() {
        return to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongRange longRange = (LongRange) o;
        return from == longRange.from && to == longRange.to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }
}
