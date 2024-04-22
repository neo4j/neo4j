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
package org.neo4j.internal.batchimport;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;

public class BackgroundFlusher implements AutoCloseable {
    private volatile boolean enabled;
    private volatile boolean halted;
    private final Thread flusher;

    public BackgroundFlusher(PageCache pageCache, long frequencyMillis) {
        this.flusher = new Thread(() -> {
            while (!halted) {
                try {
                    if (!enabled) {
                        Thread.sleep(100);
                        continue;
                    }
                    long end = System.currentTimeMillis() + frequencyMillis;
                    for (int i = 0; i < 100 && System.currentTimeMillis() < end && enabled && !halted; i++) {
                        Thread.sleep(100);
                    }
                    if (enabled && !halted) {
                        pageCache.flush(DatabaseFlushEvent.NULL);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
        this.flusher.start();
    }

    public void enable() {
        enabled = true;
    }

    public void disable() {
        enabled = false;
    }

    @Override
    public void close() {
        halted = true;
        try {
            flusher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
