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
package org.neo4j.cypher.internal.profiling;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.cypher.internal.util.attribution.Id;
import org.neo4j.cypher.result.OperatorProfile;
import org.neo4j.cypher.result.QueryProfile;
import org.neo4j.kernel.impl.query.statistic.StatisticProvider;

public class ProfilingTracer implements QueryProfiler, QueryProfile {
    public interface Clock {
        long nanoTime();

        Clock SYSTEM_TIMER = System::nanoTime;
    }

    private final Clock clock;
    private final StatisticProvider statisticProvider;
    private final Map<Integer, ProfilingTracerData> data = new HashMap<>();

    public ProfilingTracer(StatisticProvider statisticProvider) {
        this(Clock.SYSTEM_TIMER, statisticProvider);
    }

    ProfilingTracer(Clock clock, StatisticProvider statisticProvider) {
        this.clock = clock;
        this.statisticProvider = statisticProvider;
    }

    @Override
    public OperatorProfile operatorProfile(int operatorId) {
        ProfilingTracerData value = data.get(operatorId);
        if (value == null) {
            return OperatorProfile.ZERO;
        } else {
            value.sanitize();
            return value;
        }
    }

    @Override
    public long maxAllocatedMemory() {
        return OperatorProfile.NO_DATA;
    }

    public long timeOf(Id operatorId) {
        return operatorProfile(operatorId.x()).time();
    }

    public long dbHitsOf(Id operatorId) {
        return operatorProfile(operatorId.x()).dbHits();
    }

    public long rowsOf(Id operatorId) {
        return operatorProfile(operatorId.x()).rows();
    }

    @Override
    public OperatorProfileEvent executeOperator(Id operatorId) {
        return executeOperator(operatorId, true);
    }

    @Override
    public OperatorProfileEvent executeOperator(Id operatorId, boolean trackAll) {
        ProfilingTracerData operatorData = this.data.get(operatorId.x());
        if (operatorData == null) {
            operatorData = new ProfilingTracerData();
            this.data.put(operatorId.x(), operatorData);
        }
        if (trackAll) {
            return new TrackingExecutionEvent(clock, statisticProvider, operatorData, operatorId.x());
        } else {
            return new ExecutionEvent(statisticProvider, operatorData, operatorId.x());
        }
    }

    @Override
    public String toString() {
        return String.format("ProfilingTracer { %s }", data);
    }

    private static class ExecutionEvent extends OperatorProfileEvent {
        final ProfilingTracerData data;
        final StatisticProvider statisticProvider;
        long hitCount;
        long rowCount;
        int planId;

        ExecutionEvent(StatisticProvider statisticProvider, ProfilingTracerData data, int planId) {
            this.statisticProvider = statisticProvider;
            this.data = data;
            this.planId = planId;
        }

        @Override
        public void close() {
            data.update(
                    OperatorProfile.NO_DATA,
                    hitCount,
                    rowCount,
                    OperatorProfile.NO_DATA,
                    OperatorProfile.NO_DATA,
                    OperatorProfile.NO_DATA);
        }

        @Override
        public void dbHit() {
            hitCount++;
        }

        @Override
        public void dbHits(long hits) {
            hitCount += hits;
        }

        @Override
        public void row() {
            rowCount++;
        }

        @Override
        public void row(boolean hasRow) {
            if (hasRow) {
                rowCount++;
            }
        }

        @Override
        public void rows(long n) {
            rowCount += n;
        }
    }

    private static class TrackingExecutionEvent extends ExecutionEvent {
        private final long start;
        private final Clock clock;
        private long pageCountHitsStart;
        private long pageCountMissesStart;

        TrackingExecutionEvent(Clock clock, StatisticProvider statisticProvider, ProfilingTracerData data, int planId) {
            super(statisticProvider, data, planId);
            this.clock = clock;
            this.start = clock.nanoTime();
            this.pageCountHitsStart = statisticProvider.getPageCacheHits();
            this.pageCountMissesStart = statisticProvider.getPageCacheMisses();
        }

        @Override
        public void close() {
            long pageCacheHits = statisticProvider.getPageCacheHits();
            long pageCacheFaults = statisticProvider.getPageCacheMisses();
            long executionTime = clock.nanoTime() - start;
            data.update(
                    executionTime,
                    hitCount,
                    rowCount,
                    pageCacheHits - pageCountHitsStart,
                    pageCacheFaults - pageCountMissesStart,
                    OperatorProfile.NO_DATA);
        }
    }
}
