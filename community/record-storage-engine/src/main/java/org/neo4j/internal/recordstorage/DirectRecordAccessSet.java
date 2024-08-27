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
import static org.neo4j.internal.recordstorage.RecordCursorTypes.LABEL_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.REL_TYPE_TOKEN_CURSOR;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.memory.MemoryTracker;

public class DirectRecordAccessSet implements RecordAccessSet, AutoCloseable {
    private final DirectRecordAccess<NodeRecord, Void> nodeRecords;
    private final DirectRecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords;
    private final DirectRecordAccess<RelationshipRecord, Void> relationshipRecords;
    private final DirectRecordAccess<RelationshipGroupRecord, Integer> relationshipGroupRecords;
    private final DirectRecordAccess<PropertyKeyTokenRecord, Void> propertyKeyTokenRecords;
    private final DirectRecordAccess<RelationshipTypeTokenRecord, Void> relationshipTypeTokenRecords;
    private final DirectRecordAccess<LabelTokenRecord, Void> labelTokenRecords;
    private final DirectRecordAccess[] all;
    private final IdGeneratorFactory idGeneratorFactory;
    private final Loaders loaders;
    private final CachedStoreCursors storeCursors;

    public DirectRecordAccessSet(
            NeoStores neoStores,
            IdGeneratorFactory idGeneratorFactory,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        RecordStore<NodeRecord> nodeStore = neoStores.getNodeStore();
        PropertyStore propertyStore = neoStores.getPropertyStore();
        RecordStore<RelationshipRecord> relationshipStore = neoStores.getRelationshipStore();
        RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
        RecordStore<PropertyKeyTokenRecord> propertyKeyTokenStore = neoStores.getPropertyKeyTokenStore();
        RecordStore<RelationshipTypeTokenRecord> relationshipTypeTokenStore = neoStores.getRelationshipTypeTokenStore();
        RecordStore<LabelTokenRecord> labelTokenStore = neoStores.getLabelTokenStore();
        storeCursors = new CachedStoreCursors(neoStores, cursorContext);
        loaders = new Loaders(neoStores, storeCursors);
        nodeRecords = new DirectRecordAccess<>(
                nodeStore, loaders.nodeLoader(), cursorContext, NODE_CURSOR, storeCursors, memoryTracker);
        propertyRecords = new DirectRecordAccess<>(
                propertyStore, loaders.propertyLoader(), cursorContext, PROPERTY_CURSOR, storeCursors, memoryTracker);
        relationshipRecords = new DirectRecordAccess<>(
                relationshipStore,
                loaders.relationshipLoader(),
                cursorContext,
                RELATIONSHIP_CURSOR,
                storeCursors,
                memoryTracker);
        relationshipGroupRecords = new DirectRecordAccess<>(
                relationshipGroupStore,
                loaders.relationshipGroupLoader(),
                cursorContext,
                GROUP_CURSOR,
                storeCursors,
                memoryTracker);
        propertyKeyTokenRecords = new DirectRecordAccess<>(
                propertyKeyTokenStore,
                loaders.propertyKeyTokenLoader(),
                cursorContext,
                PROPERTY_KEY_TOKEN_CURSOR,
                storeCursors,
                memoryTracker);
        relationshipTypeTokenRecords = new DirectRecordAccess<>(
                relationshipTypeTokenStore,
                loaders.relationshipTypeTokenLoader(),
                cursorContext,
                REL_TYPE_TOKEN_CURSOR,
                storeCursors,
                memoryTracker);
        labelTokenRecords = new DirectRecordAccess<>(
                labelTokenStore,
                loaders.labelTokenLoader(),
                cursorContext,
                LABEL_TOKEN_CURSOR,
                storeCursors,
                memoryTracker);
        all = new DirectRecordAccess[] {
            nodeRecords,
            propertyRecords,
            relationshipRecords,
            relationshipGroupRecords,
            propertyKeyTokenRecords,
            relationshipTypeTokenRecords,
            labelTokenRecords
        };
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    public RecordAccess<NodeRecord, Void> getNodeRecords() {
        return nodeRecords;
    }

    @Override
    public RecordAccess<PropertyRecord, PrimitiveRecord> getPropertyRecords() {
        return propertyRecords;
    }

    @Override
    public RecordAccess<RelationshipRecord, Void> getRelRecords() {
        return relationshipRecords;
    }

    @Override
    public RecordAccess<RelationshipGroupRecord, Integer> getRelGroupRecords() {
        return relationshipGroupRecords;
    }

    @Override
    public RecordAccess<SchemaRecord, SchemaRule> getSchemaRuleChanges() {
        throw new UnsupportedOperationException("Not needed. Implement if needed");
    }

    @Override
    public RecordAccess<PropertyKeyTokenRecord, Void> getPropertyKeyTokenChanges() {
        return propertyKeyTokenRecords;
    }

    @Override
    public RecordAccess<LabelTokenRecord, Void> getLabelTokenChanges() {
        return labelTokenRecords;
    }

    @Override
    public RecordAccess<RelationshipTypeTokenRecord, Void> getRelationshipTypeTokenChanges() {
        return relationshipTypeTokenRecords;
    }

    public void commit() {
        commit(true);
    }

    /**
     * Commits all the changes made in this access set.
     * @param markHighId also marks {@link IdGenerator#markHighestWrittenAtHighId()} for the highest created IDs
     *                   for each record type.
     */
    public void commit(boolean markHighId) {
        for (DirectRecordAccess access : all) {
            access.commit();
        }
        if (markHighId) {
            idGeneratorFactory.visit(IdGenerator::markHighestWrittenAtHighId);
        }
    }

    @Override
    public int changeSize() {
        int total = 0;
        for (DirectRecordAccess<?, ?> access : all) {
            total += access.changeSize();
        }
        return total;
    }

    @Override
    public void close() {
        IOUtils.closeAllSilently(storeCursors);
    }
}
