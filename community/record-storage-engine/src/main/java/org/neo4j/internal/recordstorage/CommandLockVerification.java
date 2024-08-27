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

import static org.neo4j.internal.recordstorage.LockVerificationMonitor.assertRecordsEquals;
import static org.neo4j.internal.recordstorage.LockVerificationMonitor.assertSchemaLocked;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.ResourceType.NODE;
import static org.neo4j.lock.ResourceType.NODE_RELATIONSHIP_GROUP_DELETE;
import static org.neo4j.lock.ResourceType.RELATIONSHIP;
import static org.neo4j.lock.ResourceType.RELATIONSHIP_GROUP;

import java.util.Collection;
import org.neo4j.internal.recordstorage.LockVerificationMonitor.NeoStoresLoader;
import org.neo4j.internal.recordstorage.LockVerificationMonitor.StoreLoader;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

/**
 * Merely a helper during development to ensure that commands generated are sufficiently locked, now that we're experimenting with
 * more fine-granular locking.
 */
public interface CommandLockVerification {
    CommandLockVerification IGNORE = commands -> {};

    void verifySufficientlyLocked(Collection<StorageCommand> commands);

    class RealChecker implements CommandLockVerification {
        private final ResourceLocker locks;
        private final ReadableTransactionState txState;
        private final StoreLoader loader;

        RealChecker(
                ResourceLocker locks,
                ReadableTransactionState txState,
                NeoStores neoStores,
                SchemaRuleAccess schemaRuleAccess,
                StoreCursors storeCursors,
                MemoryTracker memoryTracker) {
            this.locks = locks;
            this.txState = txState;
            this.loader = new NeoStoresLoader(neoStores, schemaRuleAccess, storeCursors, memoryTracker);
        }

        @Override
        public void verifySufficientlyLocked(Collection<StorageCommand> commands) {
            commands.forEach(this::verifySufficientlyLocked);
        }

        private void verifySufficientlyLocked(StorageCommand command) {
            if (command instanceof Command.NodeCommand) {
                verifyNodeSufficientlyLocked((Command.NodeCommand) command);
            } else if (command instanceof Command.RelationshipCommand) {
                verifyRelationshipSufficientlyLocked((Command.RelationshipCommand) command);
            } else if (command instanceof Command.RelationshipGroupCommand) {
                verifyRelationshipGroupSufficientlyLocked((Command.RelationshipGroupCommand) command);
            } else if (command instanceof Command.PropertyCommand) {
                verifyPropertySufficientlyLocked((Command.PropertyCommand) command);
            } else if (command instanceof Command.SchemaRuleCommand) {
                verifySchemaSufficientlyLocked((Command.SchemaRuleCommand) command);
            }
        }

        private void verifySchemaSufficientlyLocked(Command.SchemaRuleCommand command) {
            assertSchemaLocked(locks, command.getSchemaRule(), command.before.inUse() ? command.before : command.after);
        }

        private void verifyPropertySufficientlyLocked(Command.PropertyCommand command) {
            PropertyRecord record = command.after.inUse() ? command.after : command.before;
            if (record.isNodeSet()) {
                if (!txState.nodeIsAddedInThisBatch(record.getNodeId())) {
                    assertLocked(record.getNodeId(), NODE, EXCLUSIVE, record);
                }
            } else if (record.isRelSet()) {
                if (!txState.relationshipIsAddedInThisBatch(record.getRelId())) {
                    assertLocked(record.getRelId(), RELATIONSHIP, EXCLUSIVE, record);
                }
            } else if (record.isSchemaSet()) {
                if (!command.before.inUse() && command.after.inUse()) {
                    return; // Created, we can't check anything here (might be in an inner transaction)
                }
                assertSchemaLocked(locks, loader.loadSchema(command.getSchemaRuleId()), record);
            }
        }

        private void verifyNodeSufficientlyLocked(Command.NodeCommand command) {
            long id = command.getKey();
            if (!txState.nodeIsAddedInThisBatch(id)) {
                assertLocked(id, NODE, EXCLUSIVE, command.after);
            }
            if (txState.nodeIsDeletedInThisBatch(id)) {
                assertLocked(id, NODE_RELATIONSHIP_GROUP_DELETE, EXCLUSIVE, command.after);
            }
        }

        private void verifyRelationshipSufficientlyLocked(Command.RelationshipCommand command) {
            LockVerificationMonitor.checkRelationship(txState, locks, loader, command.after);

            if (command.before.inUse()) {
                assertRecordsEquals(command.before, loader::loadRelationship);
            }
        }

        private void verifyRelationshipGroupSufficientlyLocked(Command.RelationshipGroupCommand command) {
            long node = command.after.getOwningNode();
            if (!txState.nodeIsAddedInThisBatch(node)) {
                assertLocked(node, RELATIONSHIP_GROUP, EXCLUSIVE, command.after);
            }

            boolean deleted = !command.after.inUse();
            if (deleted) {
                assertLocked(node, NODE_RELATIONSHIP_GROUP_DELETE, EXCLUSIVE, command.after);
            }

            if (command.before.inUse()) {
                assertRecordsEquals(command.before, loader::loadRelationshipGroup);
            }
        }

        private void assertLocked(long id, ResourceType resource, LockType type, AbstractBaseRecord record) {
            LockVerificationMonitor.assertLocked(locks, id, resource, type, record);
        }
    }
}
