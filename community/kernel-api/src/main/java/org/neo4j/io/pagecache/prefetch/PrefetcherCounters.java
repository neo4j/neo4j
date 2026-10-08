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
package org.neo4j.io.pagecache.prefetch;

public interface PrefetcherCounters {
    PrefetcherCounters EMPTY_PAGE_PREFETCHER_COUNTERS = new PrefetcherCounters() {
        @Override
        public long submitted(PrefetchSource source) {
            return 0;
        }

        @Override
        public long completed(PrefetchSource source) {
            return 0;
        }

        @Override
        public long pagesPrefetched(PrefetchSource source) {
            return 0;
        }
    };

    long submitted(PrefetchSource source);

    long completed(PrefetchSource source);

    long pagesPrefetched(PrefetchSource source);
}
