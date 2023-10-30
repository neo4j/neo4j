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
package org.neo4j.kernel.impl.query;

import java.util.Map;

public interface QueryCacheStatistics {
    Long preParserCacheEntries();

    Long astCacheEntries();

    Long logicalPlanCacheEntries();

    Long executionPlanCacheEntries();

    Long executableQueryCacheEntries();

    Long numberOfReplans();

    Long replanWaitTime();

    Map<String, CacheMetrics> metricsPerCacheKind();

    QueryCacheStatistics EMPTY = new QueryCacheStatistics() {
        @Override
        public Long preParserCacheEntries() {
            return 0L;
        }

        @Override
        public Long astCacheEntries() {
            return 0L;
        }

        @Override
        public Long logicalPlanCacheEntries() {
            return 0L;
        }

        @Override
        public Long executionPlanCacheEntries() {
            return 0L;
        }

        @Override
        public Long executableQueryCacheEntries() {
            return 0L;
        }

        @Override
        public Long numberOfReplans() {
            return 0L;
        }

        @Override
        public Long replanWaitTime() {
            return 0L;
        }

        @Override
        public Map<String, CacheMetrics> metricsPerCacheKind() {
            return Map.of();
        }
    };
}
