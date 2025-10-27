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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.kernel.impl.locking.LockManager;

/**
 * Manages the creation, lifecycle, and transaction IDs for lock workers.
 * This class encapsulates the TRANSACTION_ID counter that was previously static,
 * allowing for per-test isolation and management of transaction IDs.
 * It also handles creation and cleanup of LockWorker instances.
 */
public class LockWorkerManager {
    private final LockManager locks;
    private final AtomicLong transactionIdCounter;
    private final List<LockWorker> workers;

    public LockWorkerManager(LockManager locks) {
        this.locks = locks;
        this.transactionIdCounter = new AtomicLong(0);
        this.workers = new ArrayList<>();
    }

    private long getNextTransactionId() {
        return transactionIdCounter.getAndIncrement();
    }

    /**
     * Creates a new LockWorker with the given name and adds it to the managed workers list.
     *
     * @param name the name of the worker
     * @return a new LockWorker instance
     */
    public LockWorker createWorker(String name) {
        LockWorker worker = new LockWorker(name, locks, getNextTransactionId());
        workers.add(worker);
        return worker;
    }

    /**
     * Closes all managed workers.
     */
    public void closeAll() {
        for (LockWorker worker : workers) {
            worker.close();
        }
        workers.clear();
    }
}
