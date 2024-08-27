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

import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.SCHEMA_CURSOR;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;
import static org.neo4j.lock.ResourceType.NODE;
import static org.neo4j.lock.ResourceType.NODE_RELATIONSHIP_GROUP_DELETE;
import static org.neo4j.lock.ResourceType.RELATIONSHIP;
import static org.neo4j.lock.ResourceType.RELATIONSHIP_GROUP;
import static org.neo4j.lock.ResourceType.SCHEMA_NAME;
import static org.neo4j.util.Preconditions.checkState;

import java.util.Objects;
import java.util.function.LongFunction;
import org.neo4j.hashing.HashFunction;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.recordstorage.RecordAccess.LoadMonitor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

public class LockVerificationMonitor implements LoadMonitor {
    private final ResourceLocker locks;
    private final ReadableTransactionState txState;
    private final StoreLoader loader;

    LockVerificationMonitor(ResourceLocker locks, ReadableTransactionState txState, StoreLoader loader) {
        this.locks = locks;
        this.txState = txState;
        this.loader = loader;
    }

    @Override
    public void markedAsChanged(AbstractBaseRecord before) {
        // This is assuming that all before records coming here are inUse, they should really always be when getting a
        // call to this method
        if (!before.inUse()) {
            return; // we can not do anything useful with unused before records
        }

        if (before instanceof NodeRecord) {
            verifyNodeSufficientlyLocked((NodeRecord) before);
        } else if (before instanceof RelationshipRecord) {
            verifyRelationshipSufficientlyLocked((RelationshipRecord) before);
        } else if (before instanceof RelationshipGroupRecord) {
            verifyRelationshipGroupSufficientlyLocked((RelationshipGroupRecord) before);
        } else if (before instanceof PropertyRecord) {
            verifyPropertySufficientlyLocked((PropertyRecord) before);
        } else if (before instanceof SchemaRecord) {
            verifySchemaSufficientlyLocked((SchemaRecord) before);
        }
    }

    private void verifySchemaSufficientlyLocked(SchemaRecord record) {
        assertRecordsEquals(record, loader::loadSchemaRecord);
        assertSchemaLocked(locks, loader.loadSchema(record.getId()), record);
    }

    private void verifyPropertySufficientlyLocked(PropertyRecord before) {
        assertRecordsEquals(before, id -> {
            PropertyRecord stored = loader.loadProperty(
                    id); // This loads without inferred data, so just move it over so we can do equality check
            stored.setEntity(before);
            return stored;
        });

        if (before.isNodeSet()) {
            if (!txState.nodeIsAddedInThisBatch(before.getNodeId())) {
                assertLocked(before.getNodeId(), NODE, before);
            }
        } else if (before.isRelSet()) {
            if (!txState.relationshipIsAddedInThisBatch(before.getRelId())) {
                assertLocked(before.getRelId(), RELATIONSHIP, before);
            }
        } else if (before.isSchemaSet()) {
            assertSchemaLocked(locks, loader.loadSchema(before.getSchemaRuleId()), before);
        }
    }

    private void verifyNodeSufficientlyLocked(NodeRecord before) {
        assertRecordsEquals(before, loader::loadNode);
        long id = before.getId();
        if (!txState.nodeIsAddedInThisBatch(id)) {
            assertLocked(id, NODE, before);
        }
        if (txState.nodeIsDeletedInThisBatch(id)) {
            assertLocked(id, NODE_RELATIONSHIP_GROUP_DELETE, before);
        }
    }

    private void verifyRelationshipSufficientlyLocked(RelationshipRecord before) {
        assertRecordsEquals(before, loader::loadRelationship);
        long id = before.getId();
        boolean addedInThisBatch = txState.relationshipIsAddedInThisBatch(id);
        checkState(
                before.inUse() == !addedInThisBatch,
                "Relationship[%d] inUse:%b, but txState.relationshipIsAddedInThisTx:%b",
                id,
                before.inUse(),
                addedInThisBatch);
        checkRelationship(txState, locks, loader, before);
    }

    private void verifyRelationshipGroupSufficientlyLocked(RelationshipGroupRecord before) {
        assertRecordsEquals(before, loader::loadRelationshipGroup);

        long node = before.getOwningNode();
        if (!txState.nodeIsAddedInThisBatch(node)) {
            assertLocked(node, RELATIONSHIP_GROUP, before);
        }
    }

    private void assertLocked(long id, ResourceType resource, AbstractBaseRecord record) {
        assertLocked(locks, id, resource, EXCLUSIVE, record);
    }

    static void checkRelationship(
            ReadableTransactionState txState, ResourceLocker locks, StoreLoader loader, RelationshipRecord record) {
        long id = record.getId();
        if (!txState.relationshipIsAddedInThisBatch(id) && !txState.relationshipIsDeletedInThisBatch(id)) {
            // relationship only modified
            assertLocked(locks, id, RELATIONSHIP, EXCLUSIVE, record);
        } else {
            if (txState.relationshipIsDeletedInThisBatch(id)) {
                assertLocked(locks, id, RELATIONSHIP, EXCLUSIVE, record);
            } else {
                checkRelationshipNode(txState, locks, loader, record.getFirstNode());
                checkRelationshipNode(txState, locks, loader, record.getSecondNode());
            }
        }
    }

    private static void checkRelationshipNode(
            ReadableTransactionState txState, ResourceLocker locks, StoreLoader loader, long nodeId) {
        if (!txState.nodeIsAddedInThisBatch(nodeId)) {
            NodeRecord node = loader.loadNode(nodeId);
            if (node.inUse() && node.isDense()) {
                assertLocked(locks, nodeId, NODE_RELATIONSHIP_GROUP_DELETE, SHARED, node);
                checkState(
                        hasLock(locks, nodeId, NODE, EXCLUSIVE)
                                || hasLock(locks, nodeId, NODE_RELATIONSHIP_GROUP_DELETE, SHARED),
                        "%s modified w/ neither [%s,%s] nor [%s,%s]",
                        locks,
                        NODE,
                        EXCLUSIVE,
                        NODE_RELATIONSHIP_GROUP_DELETE,
                        SHARED);
            }
        }
    }

    static void assertLocked(
            ResourceLocker locks, long id, ResourceType resource, LockType type, AbstractBaseRecord record) {
        checkState(
                hasLock(locks, id, resource, type),
                "%s [%s,%s] modified without %s lock, record:%s.",
                locks,
                resource,
                id,
                type,
                record);
    }

    static void assertSchemaLocked(ResourceLocker locks, SchemaRule schemaRule, AbstractBaseRecord record) {
        if (schemaRule instanceof IndexDescriptor && ((IndexDescriptor) schemaRule).isUnique()) {
            // These are created in an inner transaction without locks. Should be protected by the parent transaction.
            // Current lock abstraction does not let us check if anyone (parent) has those locks so there is nothing we
            // can check here unfortunately
            return;
        }
        Objects.requireNonNull(schemaRule);
        assertLocked(locks, schemaNameResourceId(schemaRule.getName()), SCHEMA_NAME, EXCLUSIVE, record);

        SchemaDescriptor schema = schemaRule.schema();
        for (long key : schema.lockingKeys()) {
            assertLocked(locks, key, schema.keyType(), EXCLUSIVE, record);
        }
    }

    private static boolean hasLock(ResourceLocker locks, long id, ResourceType resource, LockType type) {
        return locks.holdsLock(id, resource, type);
    }

    private static long schemaNameResourceId(String schemaName) {
        // Copy of ResourceIds.schemaNameResourceId, as that is not accessible from in here
        final HashFunction hashFunc = HashFunction.incrementalXXH64();
        long hash = hashFunc.initialise(0x0123456789abcdefL);

        hash = schemaName.chars().asLongStream().reduce(hash, hashFunc::update);
        return hashFunc.finalise(hash);
    }

    static <RECORD extends AbstractBaseRecord> void assertRecordsEquals(RECORD before, LongFunction<RECORD> loader) {
        RECORD stored = loader.apply(before.getId());
        if (before.inUse() || stored.inUse()) {
            checkState(
                    stored.equals(before),
                    "Record which got marked as changed is not what the store has, i.e. it was read before lock was acquired%nbefore:%s%nstore:%s",
                    before,
                    stored);
        }
    }

    public interface StoreLoader {
        NodeRecord loadNode(long id);

        RelationshipRecord loadRelationship(long id);

        RelationshipGroupRecord loadRelationshipGroup(long id);

        PropertyRecord loadProperty(long id);

        SchemaRule loadSchema(long id);

        SchemaRecord loadSchemaRecord(long id);
    }

    public static class NeoStoresLoader implements StoreLoader {
        private final NeoStores neoStores;
        private final SchemaRuleAccess schemaRuleAccess;
        private final StoreCursors storeCursors;
        private final MemoryTracker memoryTracker;

        public NeoStoresLoader(
                NeoStores neoStores,
                SchemaRuleAccess schemaRuleAccess,
                StoreCursors storeCursors,
                MemoryTracker memoryTracker) {
            this.neoStores = neoStores;
            this.schemaRuleAccess = schemaRuleAccess;
            this.storeCursors = storeCursors;
            this.memoryTracker = memoryTracker;
        }

        @Override
        public NodeRecord loadNode(long id) {
            return readRecord(id, neoStores.getNodeStore(), storeCursors.readCursor(NODE_CURSOR), memoryTracker);
        }

        @Override
        public RelationshipRecord loadRelationship(long id) {
            return readRecord(
                    id, neoStores.getRelationshipStore(), storeCursors.readCursor(RELATIONSHIP_CURSOR), memoryTracker);
        }

        @Override
        public RelationshipGroupRecord loadRelationshipGroup(long id) {
            return readRecord(
                    id, neoStores.getRelationshipGroupStore(), storeCursors.readCursor(GROUP_CURSOR), memoryTracker);
        }

        @Override
        public PropertyRecord loadProperty(long id) {
            PropertyStore propertyStore = neoStores.getPropertyStore();
            PropertyRecord record =
                    readRecord(id, propertyStore, storeCursors.readCursor(PROPERTY_CURSOR), memoryTracker);
            propertyStore.ensureHeavy(record, storeCursors, memoryTracker);
            return record;
        }

        @Override
        public SchemaRule loadSchema(long id) {
            try {
                return schemaRuleAccess.loadSingleSchemaRule(id, storeCursors, memoryTracker);
            } catch (MalformedSchemaRuleException e) {
                return null;
            }
        }

        @Override
        public SchemaRecord loadSchemaRecord(long id) {
            return readRecord(id, neoStores.getSchemaStore(), storeCursors.readCursor(SCHEMA_CURSOR), memoryTracker);
        }

        private static <RECORD extends AbstractBaseRecord> RECORD readRecord(
                long id, RecordStore<RECORD> store, PageCursor pageCursor, MemoryTracker memoryTracker) {
            return store.getRecordByCursor(id, store.newRecord(), RecordLoad.ALWAYS, pageCursor, memoryTracker);
        }
    }
}
