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
package org.neo4j.dbms.diagnostics.profile;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.neo4j.time.Stopwatch;
import org.neo4j.time.SystemNanoClock;

abstract class PeriodicProfiler extends ContinuousProfiler {
    private final Duration interval;
    protected final SystemNanoClock clock;
    private Stopwatch stopwatch;

    PeriodicProfiler(Duration interval, SystemNanoClock clock) {
        this.interval = interval;
        this.clock = clock;
    }

    @Override
    protected void run(BooleanSupplier stopCondition) {
        while (!stopCondition.getAsBoolean()) {
            if (stopwatch == null || stopwatch.hasTimedOut(interval)) {
                stopwatch = clock.startStopWatch();
                tick();
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    protected abstract void tick();
}
