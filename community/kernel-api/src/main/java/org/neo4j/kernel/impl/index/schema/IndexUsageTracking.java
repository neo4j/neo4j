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

import org.neo4j.kernel.api.index.IndexUsageStats;

/**
 * Meant to be used by an {@link org.neo4j.kernel.api.index.IndexAccessor} to keep usage statistics,
 * and to later report those via {@link #getAndReset()}.
 */
public interface IndexUsageTracking {
    IndexUsageStats EMPTY_USAGE_STATS = new IndexUsageStats(0, 0, 0);

    IndexUsageTracking NO_USAGE_TRACKING = new IndexUsageTracking() {
        @Override
        public IndexUsageStats getAndReset() {
            return EMPTY_USAGE_STATS;
        }

        @Override
        public void queried() {}
    };

    IndexUsageStats getAndReset();

    void queried();
}
