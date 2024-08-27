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
package org.neo4j.internal.recordstorage;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.additional_lock_verification;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

public interface LockVerificationFactory {

    static LockVerificationFactory select(Config config) {
        boolean enabled = config.get(additional_lock_verification) && !"multiversion".equals(config.get(db_format));
        return enabled ? STRICT : NONE;
    }

    LockVerificationFactory STRICT = new StrictLockVerificationFactory();

    LockVerificationFactory NONE = new EmptyLockVerificationFactory();

    CommandLockVerification createCommandVerification(
            ResourceLocker locker,
            ReadableTransactionState txState,
            NeoStores neoStores,
            SchemaRuleAccess schemaRuleAccess,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker);

    RecordAccess.LoadMonitor createLockVerification(
            ResourceLocker locks,
            ReadableTransactionState txState,
            NeoStores neoStores,
            SchemaRuleAccess schemaRuleAccess,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker);

    class EmptyLockVerificationFactory implements LockVerificationFactory {

        @Override
        public CommandLockVerification createCommandVerification(
                ResourceLocker locker,
                ReadableTransactionState txState,
                NeoStores neoStores,
                SchemaRuleAccess schemaRuleAccess,
                StoreCursors storeCursors,
                MemoryTracker memoryTracker) {
            return CommandLockVerification.IGNORE;
        }

        @Override
        public RecordAccess.LoadMonitor createLockVerification(
                ResourceLocker locks,
                ReadableTransactionState txState,
                NeoStores neoStores,
                SchemaRuleAccess schemaRuleAccess,
                StoreCursors storeCursors,
                MemoryTracker memoryTracker) {
            return RecordAccess.LoadMonitor.NULL_MONITOR;
        }
    }

    class StrictLockVerificationFactory implements LockVerificationFactory {
        @Override
        public CommandLockVerification createCommandVerification(
                ResourceLocker locker,
                ReadableTransactionState txState,
                NeoStores neoStores,
                SchemaRuleAccess schemaRuleAccess,
                StoreCursors storeCursors,
                MemoryTracker memoryTracker) {
            return new CommandLockVerification.RealChecker(
                    locker, txState, neoStores, schemaRuleAccess, storeCursors, memoryTracker);
        }

        @Override
        public RecordAccess.LoadMonitor createLockVerification(
                ResourceLocker locks,
                ReadableTransactionState txState,
                NeoStores neoStores,
                SchemaRuleAccess schemaRuleAccess,
                StoreCursors storeCursors,
                MemoryTracker memoryTracker) {
            return new LockVerificationMonitor(
                    locks,
                    txState,
                    new LockVerificationMonitor.NeoStoresLoader(
                            neoStores, schemaRuleAccess, storeCursors, memoryTracker));
        }
    }
}
