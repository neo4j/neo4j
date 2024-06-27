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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.neo4j.internal.helpers.Format;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.time.Stopwatch;
import org.neo4j.time.SystemNanoClock;

class TimeCheckPointThreshold extends AbstractCheckPointThreshold {
    private volatile long lastCheckPointedAppendIndex;
    private volatile Duration timeout;
    private volatile Stopwatch stopWatch;

    private final long timeMillisThreshold;
    private final SystemNanoClock clock;

    TimeCheckPointThreshold(long thresholdMillis, SystemNanoClock clock) {
        super("every " + formatDuration(thresholdMillis) + " threshold");
        this.timeMillisThreshold = thresholdMillis;
        this.clock = clock;
        // The random start offset means database in a cluster will not all check-point at the same time.
        long randomStartOffset =
                thresholdMillis > 0 ? ThreadLocalRandom.current().nextLong(thresholdMillis) : 0;
        this.timeout = Duration.ofMillis(thresholdMillis + randomStartOffset);
        this.stopWatch = clock.startStopWatch();
    }

    private static String formatDuration(long thresholdMillis) {
        return Format.duration(
                thresholdMillis,
                TimeUnit.DAYS,
                TimeUnit.MILLISECONDS,
                unit -> ' ' + unit.name().toLowerCase());
    }

    @Override
    public void initialize(long appendIndex, LogPosition logPosition) {
        lastCheckPointedAppendIndex = appendIndex;
    }

    @Override
    protected boolean thresholdReached(long lastAppendIndex, LogPosition logPosition) {
        return lastAppendIndex > lastCheckPointedAppendIndex && stopWatch.hasTimedOut(timeout);
    }

    @Override
    public void checkPointHappened(long appendIndex, LogPosition logPosition) {
        lastCheckPointedAppendIndex = appendIndex;
        stopWatch = clock.startStopWatch();
        timeout = Duration.ofMillis(timeMillisThreshold);
    }

    @Override
    public long checkFrequencyMillis() {
        return timeMillisThreshold;
    }
}
