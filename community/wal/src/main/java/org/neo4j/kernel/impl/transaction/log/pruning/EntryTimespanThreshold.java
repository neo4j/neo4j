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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.io.IOException;
import java.time.Clock;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public final class EntryTimespanThreshold implements LogPruneThreshold {
    private final long timeToKeepInMillis;
    private final Clock clock;
    private final TimeUnit timeUnit;
    private final FileSizeThreshold fileSizeThreshold;
    private final InternalLog log;

    EntryTimespanThreshold(InternalLogProvider logProvider, Clock clock, TimeUnit timeUnit, long timeToKeep) {
        this(logProvider, clock, timeUnit, timeToKeep, null);
    }

    EntryTimespanThreshold(
            InternalLogProvider logProvider,
            Clock clock,
            TimeUnit timeUnit,
            long timeToKeep,
            FileSizeThreshold fileSizeThreshold) {
        this.log = logProvider.getLog(getClass());
        this.clock = clock;
        this.timeUnit = timeUnit;
        this.timeToKeepInMillis = timeUnit.toMillis(timeToKeep);
        this.fileSizeThreshold = fileSizeThreshold;
    }

    @Override
    public PrunePredicate forCycle(long lastEntryAppendIndex) {
        long lowerLimit = clock.millis() - timeToKeepInMillis;
        PrunePredicate sizeGuard = fileSizeThreshold == null ? null : fileSizeThreshold.forCycle(lastEntryAppendIndex);
        return new PrunePredicate() {
            // Time-based pruning needs the next-newer file (which was the predicate's previous call) to compare
            // its first-entry timestamp against the cutoff. Captured here so the predicate signature can stay
            // simple for every other threshold.
            private LogFileInformation previous;

            @Override
            public boolean isLowestVersionToKeep(LogFileInformation current) {
                try {
                    if (sizeGuard != null && sizeGuard.isLowestVersionToKeep(current)) {
                        return true;
                    }
                    if (previous == null) {
                        previous = current;
                        return false;
                    }
                    long ts = previous.getFirstStartRecordTimestamp();
                    boolean tripped = ts >= 0 && ts < lowerLimit;
                    previous = current;
                    return tripped;
                } catch (IOException e) {
                    long versionInError = previous == null ? -1 : previous.version();
                    log.warn("Fail to get timestamp info from transaction log file " + versionInError, e);
                    previous = current;
                    return false;
                }
            }
        };
    }

    @Override
    public String toString() {
        return timeUnit.convert(timeToKeepInMillis, TimeUnit.MILLISECONDS) + " "
                + timeUnit.name().toLowerCase(Locale.ROOT)
                + (fileSizeThreshold == null ? StringUtils.EMPTY : " " + fileSizeThreshold);
    }
}
