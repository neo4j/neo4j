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
package org.neo4j.server.http.cypher.entity;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.virtual.MapValue;

public class HttpProfilerStatistics implements ExecutionPlanDescription.ProfilerStatistics {
    private final long rows;
    private final long dbHits;
    private final long pageCacheHits;
    private final long pageCacheMisses;
    private final double pageCacheRatio;
    private final long time;

    private HttpProfilerStatistics(
            long rows, long dbHits, long pageCacheHits, long pageCacheMisses, double pageCacheRatio, long time) {
        this.rows = rows;
        this.dbHits = dbHits;
        this.pageCacheHits = pageCacheHits;
        this.pageCacheMisses = pageCacheMisses;
        this.pageCacheRatio = pageCacheRatio;
        this.time = time;
    }

    public static ExecutionPlanDescription.ProfilerStatistics fromMapValue(MapValue mapValue) {
        long dbHits = 0, rows = 0, pageCacheHits = 0, pageCacheMisses = 0, time = 0;
        double pageCacheRatio = 0;

        if (mapValue.containsKey("dbHits")) {
            dbHits = ((LongValue) mapValue.get("dbHits")).value();
        }
        if (mapValue.containsKey("pageCacheMisses")) {
            pageCacheMisses = ((LongValue) mapValue.get("pageCacheMisses")).value();
        }
        if (mapValue.containsKey("pageCacheHits")) {
            pageCacheHits = ((LongValue) mapValue.get("pageCacheHits")).value();
        }
        if (mapValue.containsKey("pageCacheRatio")) {
            pageCacheRatio = ((DoubleValue) mapValue.get("pageCacheRatio")).value();
        }
        if (mapValue.containsKey("rows")) {
            rows = ((LongValue) mapValue.get("rows")).value();
        }
        if (mapValue.containsKey("time")) {
            time = ((LongValue) mapValue.get("time")).value();
        }

        return new HttpProfilerStatistics(rows, dbHits, pageCacheHits, pageCacheMisses, pageCacheRatio, time);
    }

    @Override
    public boolean hasRows() {
        return rows > 0;
    }

    @Override
    public long getRows() {
        return rows;
    }

    @Override
    public boolean hasDbHits() {
        return dbHits > 0;
    }

    @Override
    public long getDbHits() {
        return dbHits;
    }

    @Override
    public boolean hasPageCacheStats() {
        return pageCacheHits > 0 || pageCacheMisses > 0 || pageCacheRatio > 0;
    }

    @Override
    public long getPageCacheHits() {
        return pageCacheHits;
    }

    @Override
    public long getPageCacheMisses() {
        return pageCacheMisses;
    }

    @Override
    public boolean hasTime() {
        return time > 0;
    }

    @Override
    public long getTime() {
        return time;
    }
}
