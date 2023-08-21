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

import static java.lang.String.format;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

public class JfrProfiler extends PeriodicProfiler {
    private final Path dir;
    private final Duration maxDuration;
    private final JmxDump.JfrProfileConnection jfr;

    JfrProfiler(JmxDump dump, FileSystemAbstraction fs, Path dir, Duration duration, SystemNanoClock clock) {
        super(Duration.ofSeconds(3), clock); // Check JFR status heartbeat
        this.dir = dir;
        this.maxDuration =
                duration.plus(Duration.ofMinutes(1)); // It will die at most 1 minute after duration, if we fail to stop
        try {
            fs.mkdirs(dir);
            jfr = dump.jfrConnection();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void start() {
        String fileName = format("recording-%s.jfr", clock.instant().toString());
        jfr.start("Neo4j-Profiler-Recording", maxDuration, dir.resolve(fileName));
        super.start();
    }

    @Override
    protected void stop() {
        super.stop();
        jfr.stop();
    }

    @Override
    protected void tick() {
        if (!hasRunningRecording()) {
            throw new IllegalStateException("No JFR found running. Did server die?");
        }
    }

    @Override
    protected boolean available() {
        return true; // If dump JFR connection is available (passed constructor), then we're fine
    }

    @VisibleForTesting
    boolean hasRunningRecording() {
        return jfr.isRunning();
    }
}
