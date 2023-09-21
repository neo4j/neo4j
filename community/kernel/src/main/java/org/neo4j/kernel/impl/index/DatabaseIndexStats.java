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
package org.neo4j.kernel.impl.index;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;

/**
 * The difference between this construct and the index statistics store is that the latter
 * keeps statistics by index in a persistent manner. If an index is dropped, so are its statistics.
 * These counters, in contrast, are aggregations by index type and are in-memory only (thus reset on restart).
 */
public class DatabaseIndexStats extends IndexMonitor.MonitorAdapter implements IndexCounters {
    private static class IndexTypeStats {
        final LongAdder queried = new LongAdder();
        final LongAdder populated = new LongAdder();
    }

    private final Map<IndexType, IndexTypeStats> stats;

    public DatabaseIndexStats() {
        final var stats = new EnumMap<IndexType, IndexTypeStats>(IndexType.class);
        for (final var indexType : IndexType.values()) {
            stats.put(indexType, new IndexTypeStats());
        }
        this.stats = Collections.unmodifiableMap(stats);
    }

    @Override
    public long getQueryCount(IndexType type) {
        return stats.get(type).queried.sum();
    }

    @Override
    public long getPopulationCount(IndexType type) {
        return stats.get(type).populated.sum();
    }

    public void reportQueryCount(IndexDescriptor descriptor, long queriesSinceLastReport) {
        stats.get(descriptor.getIndexType()).queried.add(queriesSinceLastReport);
    }

    @Override
    public void populationCompleteOn(IndexDescriptor descriptor) {
        stats.get(descriptor.getIndexType()).populated.increment();
    }

    @FunctionalInterface
    public interface Factory {
        DatabaseIndexStats create();
    }
}
