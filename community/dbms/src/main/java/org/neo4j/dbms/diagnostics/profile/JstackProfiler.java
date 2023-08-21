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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import org.neo4j.dbms.diagnostics.jmx.JmxDump;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.time.SystemNanoClock;

class JstackProfiler extends PeriodicProfiler {
    private final JmxDump dump;
    private final FileSystemAbstraction fs;
    private final Path dir;

    JstackProfiler(JmxDump dump, FileSystemAbstraction fs, Path dir, Duration interval, SystemNanoClock clock) {
        super(interval, clock);
        this.dump = dump;
        this.fs = fs;
        this.dir = dir;
        try {
            fs.mkdirs(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void tick() {
        String threadDump = dump.threadDump();
        if (threadDump.equals(JmxDump.THREAD_DUMP_FAILURE)) {
            throw new IllegalStateException("Failed to retrieve thread dump");
        }
        String name = String.format("threads-%s.txt", clock.instant().toString());
        try (OutputStream os = fs.openAsOutputStream(dir.resolve(name), false)) {
            os.write(threadDump.getBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean available() {
        try {
            dump.threadDump();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
