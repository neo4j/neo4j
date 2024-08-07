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

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.storageengine.api.CommandVersion;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public class NeoStoreTransactionApplierFactory implements TransactionApplierFactory {
    private final CommandVersion version;
    private final TransactionApplicationMode mode;
    private final NeoStores neoStores;
    private final CacheAccessBackDoor cacheAccess;

    NeoStoreTransactionApplierFactory(
            TransactionApplicationMode mode, NeoStores store, CacheAccessBackDoor cacheAccess) {
        this.version = mode.version();
        this.mode = mode;
        this.neoStores = store;
        this.cacheAccess = cacheAccess;
    }

    @Override
    public TransactionApplier startTx(StorageEngineTransaction transaction, BatchContext batchContext) {
        return new NeoStoreTransactionApplier(
                mode,
                version,
                neoStores,
                cacheAccess,
                batchContext,
                transaction.cursorContext(),
                transaction.storeCursors());
    }
}
