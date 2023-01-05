/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

public class JfrProfiler extends Profiler {
    static final String RECORDING_NAME = "Neo4j-Profiler-Recording";

    private final Path dir;
    private final Duration maxDuration;
    private final SystemNanoClock clock;
    private final JmxDump.JfrProfileConnection jfr;

    JfrProfiler(JmxDump dump, FileSystemAbstraction fs, Path dir, Duration maxDuration, SystemNanoClock clock) {
        this.dir = dir;
        this.maxDuration = maxDuration;
        this.clock = clock;
        try {
            fs.mkdirs(dir);
            jfr = dump.jfrConnection();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void start() {
        String fileName = format("jfr-%s.jfr", clock.instant().toString());
        jfr.start("Neo4j-Profiler-Recording", maxDuration, dir.resolve(fileName));
    }

    @Override
    protected void stop() {
        jfr.stop();
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
