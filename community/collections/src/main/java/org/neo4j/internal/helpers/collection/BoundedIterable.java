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

import java.util.Collections;
import java.util.Iterator;

public interface BoundedIterable<RECORD> extends Iterable<RECORD>, AutoCloseable {
    long UNKNOWN_MAX_COUNT = -1;

    long maxCount();

    static <T> BoundedIterable<T> empty() {
        return new BoundedIterable<>() {
            @Override
            public long maxCount() {
                return 0;
            }

            @Override
            public void close() {}

            @Override
            public Iterator<T> iterator() {
                return Collections.emptyIterator();
            }
        };
    }

    static <T> BoundedIterable<T> concat(Iterable<BoundedIterable<T>> iterables) {
        var maxCount = 0L;
        for (final var iterable : iterables) {
            final var count = iterable.maxCount();
            if (count == UNKNOWN_MAX_COUNT) {
                // If any of the iterators have an unknown max count, then the whole thing does
                maxCount = UNKNOWN_MAX_COUNT;
                break;
            }
            maxCount += iterable.maxCount();
        }

        final var finalMaxCount = maxCount;
        return new BoundedIterable<>() {
            @Override
            public long maxCount() {
                return finalMaxCount;
            }

            @Override
            public void close() throws Exception {
                Iterables.safeForAll(iterables, BoundedIterable::close);
            }

            @Override
            public Iterator<T> iterator() {
                return Iterables.concat(iterables).iterator();
            }
        };
    }
}
