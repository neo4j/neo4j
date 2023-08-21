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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockManager;

public class IndexedStoreScan implements StoreScan {
    private final LockManager.Client lockClient;
    private final StoreScan delegate;

    public IndexedStoreScan(LockManager.Client lockClient, StoreScan delegate) {
        this.lockClient = lockClient;
        this.delegate = delegate;
    }

    @Override
    public void run(ExternalUpdatesCheck externalUpdatesCheck) {
        delegate.run(externalUpdatesCheck);
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

    @Override
    public void close() {
        try {
            lockClient.close();
        } finally {
            delegate.close();
        }
    }
}
