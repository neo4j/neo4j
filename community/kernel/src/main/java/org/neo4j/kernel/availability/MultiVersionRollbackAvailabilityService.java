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
package org.neo4j.kernel.availability;

import static org.neo4j.kernel.availability.DatabaseAvailabilityGuard.ROLLBACK_REQUIREMENT;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.kernel.impl.api.ChunkedTransactionTracker;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class MultiVersionRollbackAvailabilityService extends LifecycleAdapter {

    private final DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private final ChunkedTransactionTracker chunkedTransactionTracker;
    private volatile Set<Long> transactionsToRollback;
    private volatile boolean available;

    public MultiVersionRollbackAvailabilityService(
            DatabaseAvailabilityGuard databaseAvailabilityGuard, ChunkedTransactionTracker chunkedTransactionTracker) {
        this.databaseAvailabilityGuard = databaseAvailabilityGuard;
        this.chunkedTransactionTracker = chunkedTransactionTracker;
    }

    @Override
    public void start() throws Exception {
        var transactionsToRollback = chunkedTransactionTracker.transactionsToRollback();
        if (transactionsToRollback.isEmpty()) {
            available = true;
            return;
        }
        available = false;
        databaseAvailabilityGuard.require(ROLLBACK_REQUIREMENT);
        this.transactionsToRollback = createTxIdSet(transactionsToRollback);
    }

    public void registerRollback(long transactionId) {
        if (available) {
            return;
        }
        if (transactionsToRollback.remove(transactionId) && transactionsToRollback.isEmpty()) {
            databaseAvailabilityGuard.fulfill(ROLLBACK_REQUIREMENT);
            available = true;
        }
    }

    private static Set<Long> createTxIdSet(
            Collection<ChunkedTransactionTracker.TransactionInfo> transactionsToRollback) {
        Set<Long> txIds = ConcurrentHashMap.newKeySet();
        for (ChunkedTransactionTracker.TransactionInfo transactionInfo : transactionsToRollback) {
            txIds.add(transactionInfo.transactionId());
        }
        return txIds;
    }
}
