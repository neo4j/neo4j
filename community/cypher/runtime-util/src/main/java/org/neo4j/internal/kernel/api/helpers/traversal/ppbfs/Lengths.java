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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs;

import java.util.BitSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.neo4j.memory.HeapEstimator;

/**
 * Represents the presence or absence of different types of length:
 * - Length from source
 * - Confirmed length from source (no duplicate relationships)
 * <p>
 * Implemented by interleaving multiple indexes into a single bitset, in order to conserve memory (we create many of these).
 */
public class Lengths extends BitSet {
    private static final int FACTOR = Type.values().length;

    public static final int NONE = -1;

    public static final long SHALLOW_SIZE =
            HeapEstimator.shallowSizeOfInstance(Lengths.class) + HeapEstimator.sizeOfLongArray(1);

    public enum Type {
        Source(0),
        ConfirmedSource(1);

        private final int offset;

        Type(int offset) {
            this.offset = offset;
        }
    }

    public boolean get(int index, Type type) {
        return get(index * FACTOR + type.offset);
    }

    public void set(int index, Type type) {
        set(index * FACTOR + type.offset);
    }

    public void clear(int index, Type type) {
        clear(index * FACTOR + type.offset);
    }

    public int max(Type type) {
        return stream(type).max().orElse(NONE);
    }

    public int next(int start, Type type) {
        for (int i = nextSetBit(start * FACTOR + type.offset); i != -1; i = nextSetBit(i + 1)) {
            if (i % FACTOR == type.offset) {
                return i / FACTOR;
            }
        }
        return NONE;
    }

    public int min(Type type) {
        return next(0, type);
    }

    public boolean isEmpty(Type type) {
        if (isEmpty()) {
            return true;
        }

        return min(type) == NONE;
    }

    private IntStream stream(Type type) {
        return stream().filter(i -> i % FACTOR == type.offset).map(i -> i / FACTOR);
    }

    public String renderSourceLengths() {
        return stream(Lengths.Type.Source)
                .mapToObj(i -> i + (get(i, Lengths.Type.ConfirmedSource) ? "✓" : "?"))
                .collect(Collectors.joining(",", "{", "}"));
    }
}
