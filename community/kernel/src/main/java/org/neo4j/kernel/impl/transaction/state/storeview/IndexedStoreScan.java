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
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.neo4j.kernel.impl.api.KernelTransactions.SYSTEM_TRANSACTION_ID;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.function.BooleanSupplier;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.api.LeaseService.NoLeaseClient;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.util.Preconditions;

public class IndexedStoreScan implements StoreScan {
    private final LockManager lockManager;
    private final Config config;
    private final BooleanSupplier indexExistenceChecker;
    private final StoreScan delegate;
    private final IndexDescriptor index;

    public IndexedStoreScan(
            LockManager lockManager,
            IndexDescriptor index,
            Config config,
            BooleanSupplier indexExistenceChecker,
            StoreScan delegate) {
        this.lockManager = lockManager;
        this.config = config;
        this.indexExistenceChecker = indexExistenceChecker;
        this.delegate = delegate;
        this.index = index;
    }

    @Override
    public void run(ExternalUpdatesCheck externalUpdatesCheck) {
        try (LockManager.Client client = lockManager.newClient()) {
            client.initialize(NoLeaseClient.INSTANCE, SYSTEM_TRANSACTION_ID, INSTANCE, config);
            client.acquireShared(
                    LockTracer.NONE, index.schema().keyType(), index.schema().lockingKeys());
            Preconditions.checkState(indexExistenceChecker.getAsBoolean(), "%s no longer exists", index);
            delegate.run(externalUpdatesCheck);
        }
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public PopulationProgress getProgress() {
        return delegate.getProgress();
    }

    @Override
    public void setPhaseTracker(PhaseTracker phaseTracker) {
        delegate.setPhaseTracker(phaseTracker);
    }
}
