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
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.kernel.impl.api.LeaseService.NoLeaseClient;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.memory.EmptyMemoryTracker;

class LockWorkerState {
    private static final AtomicLong TRANSACTION_ID = new AtomicLong();
    final LockManager grabber;
    final LockManager.Client client;
    final List<String> completedOperations = new ArrayList<>();
    String doing;

    LockWorkerState(LockManager locks) {
        this.grabber = locks;
        this.client = locks.newClient();
        this.client.initialize(
                NoLeaseClient.INSTANCE,
                TRANSACTION_ID.getAndIncrement(),
                EmptyMemoryTracker.INSTANCE,
                Config.defaults(GraphDatabaseInternalSettings.lock_manager_verbose_deadlocks, true));
    }

    public void doing(String doing) {
        this.doing = doing;
    }

    public void done() {
        this.completedOperations.add(this.doing);
        this.doing = null;
    }
}
