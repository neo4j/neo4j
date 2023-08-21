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
package org.neo4j.kernel.impl.locking.forseti;

import static org.neo4j.lock.ResourceType.NODE;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.test.OtherThreadExecutor;

public class LockWorker extends OtherThreadExecutor {
    private final LockWorkerState state;

    public LockWorker(String name, LockManager locks) {
        super(name);
        state = new LockWorkerState(locks);
    }

    private Future<Void> perform(Callable<Void> acquireLockCommand, boolean wait) throws Exception {
        Future<Void> future = executeDontWait(acquireLockCommand);
        if (wait) {
            awaitFuture(future);
        } else {
            waitUntilWaiting();
        }
        return future;
    }

    public Future<Void> getReadLock(final long resource, final boolean wait) throws Exception {
        return perform(
                () -> {
                    state.doing("+R " + resource + ", wait:" + wait);
                    state.client.acquireShared(LockTracer.NONE, NODE, resource);
                    state.done();
                    return null;
                },
                wait);
    }

    public Future<Void> getWriteLock(final long resource, final boolean wait) throws Exception {
        return perform(
                () -> {
                    state.doing("+W " + resource + ", wait:" + wait);
                    state.client.acquireExclusive(LockTracer.NONE, NODE, resource);
                    state.done();
                    return null;
                },
                wait);
    }

    public void releaseReadLock(final long resource) throws Exception {
        perform(
                () -> {
                    state.doing("-R " + resource);
                    state.client.releaseShared(NODE, resource);
                    state.done();
                    return null;
                },
                true);
    }

    public void releaseWriteLock(final long resource) throws Exception {
        perform(
                () -> {
                    state.doing("-W " + resource);
                    state.client.releaseExclusive(NODE, resource);
                    state.done();
                    return null;
                },
                true);
    }
}
