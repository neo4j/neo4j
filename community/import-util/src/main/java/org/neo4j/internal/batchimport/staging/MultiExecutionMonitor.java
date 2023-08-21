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
package org.neo4j.internal.batchimport.staging;

import java.util.Arrays;
import java.util.stream.LongStream;
import org.neo4j.common.DependencyResolver;

/**
 * {@link ExecutionMonitor} that wraps several other monitors. Each wrapped monitor can still specify
 * individual poll frequencies and this {@link MultiExecutionMonitor} will make that happen.
 */
public class MultiExecutionMonitor implements ExecutionMonitor {
    private final ExecutionMonitor[] monitors;
    private final long[] nextIntervals;
    private final long checkInterval;

    public MultiExecutionMonitor(ExecutionMonitor... monitors) {
        this.monitors = monitors;
        this.nextIntervals = Arrays.stream(monitors)
                .mapToLong(ExecutionMonitor::checkIntervalMillis)
                .toArray();
        this.checkInterval = LongStream.of(nextIntervals).min().orElse(0);
    }

    @Override
    public void initialize(DependencyResolver dependencyResolver) {
        for (ExecutionMonitor monitor : monitors) {
            monitor.initialize(dependencyResolver);
        }
    }

    @Override
    public void start(StageExecution execution) {
        for (ExecutionMonitor monitor : monitors) {
            monitor.start(execution);
        }
    }

    @Override
    public void end(StageExecution execution, long totalTimeMillis) {
        for (ExecutionMonitor monitor : monitors) {
            monitor.end(execution, totalTimeMillis);
        }
    }

    @Override
    public void done(boolean successful, long totalTimeMillis, String additionalInformation) {
        for (ExecutionMonitor monitor : monitors) {
            monitor.done(successful, totalTimeMillis, additionalInformation);
        }
    }

    @Override
    public long checkIntervalMillis() {
        return checkInterval;
    }

    @Override
    public void check(StageExecution execution) {
        for (int i = 0; i < nextIntervals.length; i++) {
            nextIntervals[i] -= checkInterval;
            if (nextIntervals[i] <= 0) {
                monitors[i].check(execution);
                nextIntervals[i] += monitors[i].checkIntervalMillis();
            }
        }
    }
}
