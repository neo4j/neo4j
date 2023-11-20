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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.LeaseService.NoLeaseClient;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;

@ActorsExtension
@TestDirectoryExtension
public abstract class LockCompatibilityTestSupport {
    @Inject
    public Actor threadA;

    @Inject
    public Actor threadB;

    @Inject
    public Actor threadC;

    @Inject
    public TestDirectory testDir;

    protected final LockingCompatibilityTest suite;

    protected LockManager locks;
    protected LockManager.Client clientA;
    protected LockManager.Client clientB;
    protected LockManager.Client clientC;

    private final Map<LockManager.Client, Actor> clientToThreadMap = new HashMap<>();

    public LockCompatibilityTestSupport(LockingCompatibilityTest suite) {
        this.suite = suite;
    }

    @BeforeEach
    public void before() {
        locks = suite.createLockManager(Config.defaults(), Clocks.nanoClock());
        clientA = locks.newClient();
        clientB = locks.newClient();
        clientC = locks.newClient();
        clientA.initialize(NoLeaseClient.INSTANCE, 1, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientB.initialize(NoLeaseClient.INSTANCE, 2, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientC.initialize(NoLeaseClient.INSTANCE, 3, EmptyMemoryTracker.INSTANCE, Config.defaults());

        clientToThreadMap.put(clientA, threadA);
        clientToThreadMap.put(clientB, threadB);
        clientToThreadMap.put(clientC, threadC);
    }

    @AfterEach
    public void after() {
        clientA.close();
        clientB.close();
        clientC.close();
        locks.close();
        clientToThreadMap.clear();
    }

    // Utilities

    public abstract static class LockCommand implements Runnable {
        private final Actor thread;
        private final LockManager.Client client;

        LockCommand(Actor thread, LockManager.Client client) {
            this.thread = thread;
            this.client = client;
        }

        public Future<Void> call() {
            return thread.submit(this);
        }

        Future<Void> callAndAssertWaiting() {
            Future<Void> otherThreadLock = call();
            try {
                thread.untilWaiting();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            assertFalse(otherThreadLock.isDone(), "Should not have acquired lock.");
            return otherThreadLock;
        }

        @Override
        public void run() {
            doWork(client);
        }

        abstract void doWork(LockManager.Client client);

        public LockManager.Client client() {
            return client;
        }
    }

    protected LockCommand acquireExclusive(
            final LockManager.Client client, final LockTracer tracer, final ResourceType resourceType, final long key) {
        return new LockCommand(clientToThreadMap.get(client), client) {
            @Override
            public void doWork(LockManager.Client client) {
                client.acquireExclusive(tracer, resourceType, key);
            }
        };
    }

    protected LockCommand acquireShared(
            LockManager.Client client, final LockTracer tracer, final ResourceType resourceType, final long key) {
        return new LockCommand(clientToThreadMap.get(client), client) {
            @Override
            public void doWork(LockManager.Client client) {
                client.acquireShared(tracer, resourceType, key);
            }
        };
    }

    protected LockCommand release(final LockManager.Client client, final ResourceType resourceType, final long key) {
        return new LockCommand(clientToThreadMap.get(client), client) {
            @Override
            public void doWork(LockManager.Client client) {
                client.releaseExclusive(resourceType, key);
            }
        };
    }

    static void assertNotWaiting(Future<Void> lock) {
        assertDoesNotThrow(() -> lock.get(5, TimeUnit.SECONDS), "Waiting for lock timed out!");
    }

    void assertWaiting(LockManager.Client client, Future<Void> lock) {
        assertThrows(TimeoutException.class, () -> lock.get(10, TimeUnit.MILLISECONDS));
        assertDoesNotThrow(() -> clientToThreadMap.get(client).untilWaiting());
    }
}
