/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage;

import org.apache.commons.lang3.mutable.MutableInt;

import org.neo4j.internal.recordstorage.RecordAccess.Loader;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.memory.MemoryTracker;

public class RecordChangeSet implements RecordAccessSet
{
    private final RecordAccess<NodeRecord> nodeRecords;
    private final RecordAccess<PropertyRecord> propertyRecords;
    private final RecordAccess<RelationshipRecord> relRecords;
    private final RecordAccess<RelationshipGroupRecord> relGroupRecords;
    private final RecordAccess<SchemaRecord> schemaRuleChanges;
    private final RecordAccess<PropertyKeyTokenRecord> propertyKeyTokenChanges;
    private final RecordAccess<LabelTokenRecord> labelTokenChanges;
    private final RecordAccess<RelationshipTypeTokenRecord> relationshipTypeTokenChanges;
    private final MutableInt changeCounter = new MutableInt();

    public RecordChangeSet( Loaders loaders, MemoryTracker memoryTracker )
    {
        this( loaders.nodeLoader(),
              loaders.propertyLoader(),
              loaders.relationshipLoader(),
              loaders.relationshipGroupLoader(),
              loaders.schemaRuleLoader(),
              loaders.propertyKeyTokenLoader(),
              loaders.labelTokenLoader(),
              loaders.relationshipTypeTokenLoader(),
              memoryTracker );
    }

    public RecordChangeSet(
            Loader<NodeRecord> nodeLoader,
            Loader<PropertyRecord> propertyLoader,
            Loader<RelationshipRecord> relationshipLoader,
            Loader<RelationshipGroupRecord> relationshipGroupLoader,
            Loader<SchemaRecord> schemaRuleLoader,
            Loader<PropertyKeyTokenRecord> propertyKeyTokenLoader,
            Loader<LabelTokenRecord> labelTokenLoader,
            Loader<RelationshipTypeTokenRecord> relationshipTypeTokenLoader,
            MemoryTracker memoryTracker )
    {
        this.nodeRecords = new RecordChanges<>( nodeLoader, changeCounter, memoryTracker );
        this.propertyRecords = new RecordChanges<>( propertyLoader, changeCounter, memoryTracker );
        this.relRecords = new RecordChanges<>( relationshipLoader, changeCounter, memoryTracker );
        this.relGroupRecords = new RecordChanges<>( relationshipGroupLoader, changeCounter, memoryTracker );
        this.schemaRuleChanges = new RecordChanges<>( schemaRuleLoader, changeCounter, memoryTracker );
        this.propertyKeyTokenChanges = new RecordChanges<>( propertyKeyTokenLoader, changeCounter, memoryTracker );
        this.labelTokenChanges = new RecordChanges<>( labelTokenLoader, changeCounter, memoryTracker );
        this.relationshipTypeTokenChanges = new RecordChanges<>( relationshipTypeTokenLoader, changeCounter, memoryTracker );
    }

    @Override
    public RecordAccess<NodeRecord> getNodeRecords()
    {
        return nodeRecords;
    }

    @Override
    public RecordAccess<PropertyRecord> getPropertyRecords()
    {
        return propertyRecords;
    }

    @Override
    public RecordAccess<RelationshipRecord> getRelRecords()
    {
        return relRecords;
    }

    @Override
    public RecordAccess<RelationshipGroupRecord> getRelGroupRecords()
    {
        return relGroupRecords;
    }

    @Override
    public RecordAccess<SchemaRecord> getSchemaRuleChanges()
    {
        return schemaRuleChanges;
    }

    @Override
    public RecordAccess<PropertyKeyTokenRecord> getPropertyKeyTokenChanges()
    {
        return propertyKeyTokenChanges;
    }

    @Override
    public RecordAccess<LabelTokenRecord> getLabelTokenChanges()
    {
        return labelTokenChanges;
    }

    @Override
    public RecordAccess<RelationshipTypeTokenRecord> getRelationshipTypeTokenChanges()
    {
        return relationshipTypeTokenChanges;
    }

    @Override
    public boolean hasChanges()
    {
        return changeCounter.intValue() > 0;
    }

    @Override
    public int changeSize()
    {
        return changeCounter.intValue();
    }
}
