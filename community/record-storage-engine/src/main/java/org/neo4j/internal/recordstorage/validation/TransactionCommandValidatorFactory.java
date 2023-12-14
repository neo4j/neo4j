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
package org.neo4j.internal.recordstorage.validation;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidator;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory;

public class TransactionCommandValidatorFactory implements TransactionValidatorFactory {
    private final NeoStores neoStores;
    private final LogProvider logProvider;
    private final LockManager lockManager;
    private final Config config;

    public TransactionCommandValidatorFactory(
            NeoStores neoStores, Config config, LockManager lockManager, LogProvider logProvider) {
        this.neoStores = neoStores;
        this.lockManager = lockManager;
        this.logProvider = logProvider;
        this.config = config;
    }

    @Override
    public TransactionValidator createTransactionValidator(
            MemoryTracker memoryTracker, TransactionMonitor transactionMonitor) {
        return new TransactionCommandValidator(
                neoStores, lockManager, memoryTracker, config, logProvider, transactionMonitor);
    }
}
