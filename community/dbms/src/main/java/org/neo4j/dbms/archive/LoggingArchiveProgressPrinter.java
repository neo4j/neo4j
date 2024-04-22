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
package org.neo4j.dbms.archive;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.neo4j.dbms.archive.printer.OutputProgressPrinter;
import org.neo4j.dbms.archive.printer.ProgressPrinters.EmptyOutputProgressPrinter;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.ByteUnit;

class LoggingArchiveProgressPrinter implements ArchiveProgressPrinter {
    public static final Duration PRINT_INTERVAL = Duration.ofSeconds(60);
    private final OutputProgressPrinter progressPrinter;
    private final Supplier<Instant> timeSource;

    private long currentBytes;
    private long currentFiles;
    private boolean done;
    private long maxBytes;
    private long maxFiles;

    private boolean force;
    private Deadline deadline = null;
    private PercentageCondition percentage = null;

    public static ArchiveProgressPrinter createProgressPrinter(
            OutputProgressPrinter progressPrinter, Supplier<Instant> timeSource) {
        requireNonNull(progressPrinter);
        if (progressPrinter instanceof EmptyOutputProgressPrinter) {
            return ArchiveProgressPrinter.EMPTY;
        }
        return new LoggingArchiveProgressPrinter(progressPrinter, timeSource);
    }

    private LoggingArchiveProgressPrinter(OutputProgressPrinter progressPrinter, Supplier<Instant> timeSource) {
        this.progressPrinter = requireNonNull(progressPrinter);
        this.timeSource = requireNonNull(timeSource);
    }

    @Override
    public Resource startPrinting() {
        deadline = new Deadline(Instant.EPOCH, PRINT_INTERVAL);
        return () -> {
            done();
            printProgress();
        };
    }

    @Override
    public void reset() {
        maxBytes = 0;
        maxFiles = 0;
        currentBytes = 0;
        currentFiles = 0;
        deadline = null;
        percentage = null;
    }

    @Override
    public void maxBytes(long value) {
        maxBytes = value;
        percentage = new PercentageCondition(value);
    }

    @Override
    public long maxBytes() {
        return maxBytes;
    }

    @Override
    public void maxFiles(long value) {
        maxFiles = value;
    }

    @Override
    public long maxFiles() {
        return maxFiles;
    }

    @Override
    public void beginFile() {
        currentFiles++;
    }

    @Override
    public void printOnNextUpdate() {
        force = true;
    }

    @Override
    public void addBytes(long n) {
        currentBytes += n;

        var when = timeSource.get();
        var deadlineReached = (deadline != null && deadline.reached(when));
        var percentageReached = (percentage != null && percentage.updateAndCheckIfReached(currentBytes));

        if (force || deadlineReached || percentageReached) {
            printProgress();

            // If we manage to print, for whatever reason, move the deadline into the future.
            if (deadline != null) {
                deadline.next(when);
            }
            force = false;
        }
    }

    @Override
    public void endFile() {
        printProgress();
    }

    @Override
    public void done() {
        done = true;
    }

    @Override
    public void printProgress() {
        if (done) {
            progressPrinter.print(
                    "Done: " + currentFiles + " files, " + ByteUnit.bytesToString(currentBytes) + " processed.");
            progressPrinter.complete();
        } else if (maxFiles > 0 && maxBytes > 0) {
            double progress = (currentBytes / (double) maxBytes) * 100;
            progressPrinter.print(
                    "Files: " + currentFiles + '/' + maxFiles + ", data: " + String.format("%4.1f%%", progress));
        } else {
            progressPrinter.print("Files: " + currentFiles + "/?" + ", data: ??.?%");
        }
    }

    static class Deadline {
        private final Duration interval;
        private Instant target;

        Deadline(Instant now, Duration interval) {
            this.interval = interval;
            this.target = increment(now, interval);
        }

        boolean reached(Instant when) {
            return when.isAfter(target);
        }

        void next(Instant now) {
            target = increment(now, interval);
        }

        private static Instant increment(Instant target, Duration duration) {
            return target.plusMillis(duration.toMillis());
        }
    }

    static class PercentageCondition {
        final long bucket;
        long current;

        PercentageCondition(long maxBytes) {
            bucket = maxBytes / 100;
            current = 0;
        }

        boolean updateAndCheckIfReached(long currentBytes) {
            // If we have less than 100 bytes, disable the check
            if (bucket == 0) {
                return false;
            }

            long previous = current;
            current = currentBytes / bucket;
            return current > previous;
        }
    }
}
