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
package org.neo4j.test.extension;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.test.OtherThreadExecutor;

public class OtherThread {
    private String name;
    private long timeout;
    private TimeUnit unit;
    private volatile OtherThreadExecutor executor;

    public OtherThread() {
        this(null);
    }

    private OtherThread(String name) {
        set(name, 60, SECONDS);
    }

    public void set(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
    }

    public void set(String name, long timeout, TimeUnit unit) {
        this.name = name;
        this.timeout = timeout;
        this.unit = unit;
    }

    public <RESULT> Future<RESULT> execute(Callable<RESULT> cmd) {
        Future<RESULT> future = executor.executeDontWait(cmd);
        try {
            executor.awaitStartExecuting();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while awaiting start of execution.", e);
        }
        return future;
    }

    public OtherThreadExecutor get() {
        return executor;
    }

    public void interrupt() {
        executor.interrupt();
    }

    @Override
    public String toString() {
        OtherThreadExecutor otherThread = executor;
        if (otherThread == null) {
            return "OtherThreadRule[state=dead]";
        }
        return otherThread.toString();
    }

    // Implementation of life cycles

    public void beforeEach(ExtensionContext context) {
        String displayName = context.getDisplayName();

        String threadName = name != null ? name + '-' + displayName : displayName;
        init(threadName);
    }

    public void afterEach() {
        try {
            executor.close();
        } finally {
            executor = null;
        }
    }

    public void init(String threadName) {
        executor = new OtherThreadExecutor(threadName, timeout, unit);
    }

    public void close() {
        executor.close();
    }
}
