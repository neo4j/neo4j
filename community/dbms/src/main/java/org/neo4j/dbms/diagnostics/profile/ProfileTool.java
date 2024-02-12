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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.neo4j.internal.helpers.Exceptions;

class ProfileTool implements AutoCloseable {
    private final List<Profiler> profilers = new ArrayList<>();
    private boolean running;

    boolean add(Profiler profiler) {
        return profiler.available() && profilers.add(profiler);
    }

    synchronized void start() {
        if (!running) {
            running = true;
            safeProfilerOperation(Profiler::startProfiling);
        }
    }

    synchronized void stop() {
        if (running) {
            running = false;
            safeProfilerOperation(Profiler::stopProfiling);
        }
    }

    @Override
    public void close() {
        stop();
    }

    boolean hasProfilers() {
        return !profilers.isEmpty();
    }

    boolean hasRunningProfilers() {
        return running && profilers.stream().anyMatch(profiler -> profiler.failure() == null);
    }

    Iterable<Profiler> profilers() {
        return profilers;
    }

    private void safeProfilerOperation(Consumer<Profiler> profilerOperation) {
        RuntimeException exception = null;

        for (Profiler profiler : profilers) {
            try {
                profilerOperation.accept(profiler);
            } catch (RuntimeException e) {
                exception = Exceptions.chain(exception, e);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}
