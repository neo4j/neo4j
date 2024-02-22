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

import static org.neo4j.configuration.GraphDatabaseInternalSettings.multi_version_dump_transaction_validation_page_locks;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidator;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory;
import org.neo4j.storageengine.api.txstate.validation.ValidationLockDumper;

public class TransactionCommandValidatorFactory implements TransactionValidatorFactory {
    private final NeoStores neoStores;
    private final Config config;
    private final LogProvider logProvider;

    public TransactionCommandValidatorFactory(NeoStores neoStores, Config config, LogProvider logProvider) {
        this.neoStores = neoStores;
        this.config = config;
        this.logProvider = logProvider;
    }

    @Override
    public TransactionValidator createTransactionValidator(
            MemoryTracker memoryTracker, TransactionMonitor transactionMonitor) {
        return new TransactionCommandValidator(neoStores, config, transactionMonitor);
    }

    @Override
    public ValidationLockDumper createValidationLockDumper() {
        return config.get(multi_version_dump_transaction_validation_page_locks)
                ? new VerboseValidationLogDumper(logProvider, neoStores)
                : ValidationLockDumper.EMPTY_DUMPER;
    }
}
