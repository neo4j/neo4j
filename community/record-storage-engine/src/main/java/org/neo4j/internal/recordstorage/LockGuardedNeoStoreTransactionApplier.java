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

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockType;
import org.neo4j.storageengine.api.CommandVersion;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Visits commands targeted towards the {@link NeoStores} and update corresponding stores.
 * What happens in here is what will happen in a "internal" transaction, i.e. a transaction that has been
 * forged in this database, with transaction state, a KernelTransaction and all that and is now committing.
 * <p>
 * For other modes of application, like recovery or external there are other, added functionality, decorated
 * outside this applier.
 */
public class LockGuardedNeoStoreTransactionApplier extends NeoStoreTransactionApplier {
    private final LockGroup lockGroup;
    private final LockService lockService;

    public LockGuardedNeoStoreTransactionApplier(
            TransactionApplicationMode mode,
            CommandVersion version,
            NeoStores neoStores,
            CacheAccessBackDoor cacheAccess,
            LockService lockService,
            BatchContext batchContext,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        super(mode, version, neoStores, cacheAccess, batchContext, cursorContext, storeCursors);
        this.lockGroup = batchContext.getLockGroup();
        this.lockService = lockService;
    }

    @Override
    public boolean visitNodeCommand(Command.NodeCommand command) {
        // acquire lock
        lockGroup.add(lockService.acquireNodeLock(command.getKey(), LockType.EXCLUSIVE));
        return super.visitNodeCommand(command);
    }

    @Override
    public boolean visitRelationshipCommand(Command.RelationshipCommand command) {
        lockGroup.add(lockService.acquireRelationshipLock(command.getKey(), LockType.EXCLUSIVE));

        return super.visitRelationshipCommand(command);
    }

    @Override
    public boolean visitPropertyCommand(Command.PropertyCommand command) {
        // acquire lock
        if (command.after.isNodeSet()) {
            lockGroup.add(lockService.acquireNodeLock(command.getNodeId(), LockType.EXCLUSIVE));
        } else if (command.after.isRelSet()) {
            lockGroup.add(lockService.acquireRelationshipLock(command.getRelId(), LockType.EXCLUSIVE));
        } else if (command.after.isSchemaSet()) {
            lockGroup.add(lockService.acquireCustomLock(
                    Command.RECOVERY_LOCK_TYPE_SCHEMA_RULE, command.getSchemaRuleId(), LockType.EXCLUSIVE));
        }

        return super.visitPropertyCommand(command);
    }
}
