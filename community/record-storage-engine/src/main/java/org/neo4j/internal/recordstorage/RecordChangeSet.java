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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.internal.recordstorage.RecordAccess.Loader;
import org.neo4j.internal.schema.SchemaRule;
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
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class RecordChangeSet implements RecordAccessSet {
    private final RecordAccess<NodeRecord, Void> nodeRecords;
    private final RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords;
    private final RecordAccess<RelationshipRecord, Void> relRecords;
    private final RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords;
    private final RecordAccess<SchemaRecord, SchemaRule> schemaRuleChanges;
    private final RecordAccess<PropertyKeyTokenRecord, Void> propertyKeyTokenChanges;
    private final RecordAccess<LabelTokenRecord, Void> labelTokenChanges;
    private final RecordAccess<RelationshipTypeTokenRecord, Void> relationshipTypeTokenChanges;
    private final MutableInt changeCounter = new MutableInt();

    public RecordChangeSet(
            Loaders loaders, MemoryTracker memoryTracker, RecordAccess.LoadMonitor monitor, StoreCursors storeCursors) {
        this(
                loaders.nodeLoader(),
                loaders.propertyLoader(),
                loaders.relationshipLoader(),
                loaders.relationshipGroupLoader(),
                loaders.schemaRuleLoader(),
                loaders.propertyKeyTokenLoader(),
                loaders.labelTokenLoader(),
                loaders.relationshipTypeTokenLoader(),
                memoryTracker,
                monitor,
                storeCursors);
    }

    public RecordChangeSet(
            Loader<NodeRecord, Void> nodeLoader,
            Loader<PropertyRecord, PrimitiveRecord> propertyLoader,
            Loader<RelationshipRecord, Void> relationshipLoader,
            Loader<RelationshipGroupRecord, Integer> relationshipGroupLoader,
            Loader<SchemaRecord, SchemaRule> schemaRuleLoader,
            Loader<PropertyKeyTokenRecord, Void> propertyKeyTokenLoader,
            Loader<LabelTokenRecord, Void> labelTokenLoader,
            Loader<RelationshipTypeTokenRecord, Void> relationshipTypeTokenLoader,
            MemoryTracker memoryTracker,
            RecordAccess.LoadMonitor monitor,
            StoreCursors storeCursors) {
        this.nodeRecords = RecordChanges.create(nodeLoader, changeCounter, memoryTracker, monitor, storeCursors);
        this.propertyRecords =
                RecordChanges.create(propertyLoader, changeCounter, memoryTracker, monitor, storeCursors);
        this.relRecords = RecordChanges.create(relationshipLoader, changeCounter, memoryTracker, monitor, storeCursors);
        this.relGroupRecords =
                RecordChanges.create(relationshipGroupLoader, changeCounter, memoryTracker, monitor, storeCursors);
        this.schemaRuleChanges =
                RecordChanges.create(schemaRuleLoader, changeCounter, memoryTracker, monitor, storeCursors);
        this.propertyKeyTokenChanges =
                RecordChanges.create(propertyKeyTokenLoader, changeCounter, memoryTracker, monitor, storeCursors);
        this.labelTokenChanges =
                RecordChanges.create(labelTokenLoader, changeCounter, memoryTracker, monitor, storeCursors);
        this.relationshipTypeTokenChanges =
                RecordChanges.create(relationshipTypeTokenLoader, changeCounter, memoryTracker, monitor, storeCursors);
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
        return relRecords;
    }

    @Override
    public RecordAccess<RelationshipGroupRecord, Integer> getRelGroupRecords() {
        return relGroupRecords;
    }

    @Override
    public RecordAccess<SchemaRecord, SchemaRule> getSchemaRuleChanges() {
        return schemaRuleChanges;
    }

    @Override
    public RecordAccess<PropertyKeyTokenRecord, Void> getPropertyKeyTokenChanges() {
        return propertyKeyTokenChanges;
    }

    @Override
    public RecordAccess<LabelTokenRecord, Void> getLabelTokenChanges() {
        return labelTokenChanges;
    }

    @Override
    public RecordAccess<RelationshipTypeTokenRecord, Void> getRelationshipTypeTokenChanges() {
        return relationshipTypeTokenChanges;
    }

    @Override
    public int changeSize() {
        return changeCounter.intValue();
    }
}
