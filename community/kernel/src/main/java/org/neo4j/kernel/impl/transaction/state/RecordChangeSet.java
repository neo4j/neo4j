/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.Loader;
import org.neo4j.kernel.impl.util.statistics.IntCounter;
import org.neo4j.storageengine.api.schema.SchemaRule;

public class RecordChangeSet implements RecordAccessSet
{
    private final RecordAccess<NodeRecord, Void> nodeRecords;
    private final RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords;
    private final RecordAccess<RelationshipRecord, Void> relRecords;
    private final RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords;
    private final RecordAccess<SchemaRecord, SchemaRule> schemaRuleChanges;
    private final RecordAccess<PropertyKeyTokenRecord, Void> propertyKeyTokenChanges;
    private final RecordAccess<LabelTokenRecord, Void> labelTokenChanges;
    private final RecordAccess<RelationshipTypeTokenRecord, Void> relationshipTypeTokenChanges;
    private final IntCounter changeCounter = new IntCounter();

    public RecordChangeSet( Loaders loaders )
    {
        this(   loaders.nodeLoader(),
                loaders.propertyLoader(),
                loaders.relationshipLoader(),
                loaders.relationshipGroupLoader(),
                loaders.schemaRuleLoader(),
                loaders.propertyKeyTokenLoader(),
                loaders.labelTokenLoader(),
                loaders.relationshipTypeTokenLoader() );
    }

    public RecordChangeSet(
            Loader<NodeRecord,Void> nodeLoader,
            Loader<PropertyRecord,PrimitiveRecord> propertyLoader,
            Loader<RelationshipRecord,Void> relationshipLoader,
            Loader<RelationshipGroupRecord,Integer> relationshipGroupLoader,
            Loader<SchemaRecord,SchemaRule> schemaRuleLoader,
            Loader<PropertyKeyTokenRecord,Void> propertyKeyTokenLoader,
            Loader<LabelTokenRecord,Void> labelTokenLoader,
            Loader<RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader )
    {
        this.nodeRecords = new RecordChanges<>( nodeLoader, changeCounter );
        this.propertyRecords = new RecordChanges<>( propertyLoader, changeCounter );
        this.relRecords = new RecordChanges<>( relationshipLoader, changeCounter );
        this.relGroupRecords = new RecordChanges<>( relationshipGroupLoader, changeCounter );
        this.schemaRuleChanges = new RecordChanges<>( schemaRuleLoader, changeCounter );
        this.propertyKeyTokenChanges = new RecordChanges<>( propertyKeyTokenLoader, changeCounter );
        this.labelTokenChanges = new RecordChanges<>( labelTokenLoader, changeCounter );
        this.relationshipTypeTokenChanges = new RecordChanges<>( relationshipTypeTokenLoader, changeCounter );
    }

    @Override
    public RecordAccess<NodeRecord, Void> getNodeRecords()
    {
        return nodeRecords;
    }

    @Override
    public RecordAccess<PropertyRecord, PrimitiveRecord> getPropertyRecords()
    {
        return propertyRecords;
    }

    @Override
    public RecordAccess<RelationshipRecord, Void> getRelRecords()
    {
        return relRecords;
    }

    @Override
    public RecordAccess<RelationshipGroupRecord, Integer> getRelGroupRecords()
    {
        return relGroupRecords;
    }

    @Override
    public RecordAccess<SchemaRecord, SchemaRule> getSchemaRuleChanges()
    {
        return schemaRuleChanges;
    }

    @Override
    public RecordAccess<PropertyKeyTokenRecord, Void> getPropertyKeyTokenChanges()
    {
        return propertyKeyTokenChanges;
    }

    @Override
    public RecordAccess<LabelTokenRecord, Void> getLabelTokenChanges()
    {
        return labelTokenChanges;
    }

    @Override
    public RecordAccess<RelationshipTypeTokenRecord, Void> getRelationshipTypeTokenChanges()
    {
        return relationshipTypeTokenChanges;
    }

    @Override
    public boolean hasChanges()
    {
        return changeCounter.value() > 0;
    }

    @Override
    public int changeSize()
    {
        return changeCounter.value();
    }

    @Override
    public void close()
    {
        if ( hasChanges() )
        {
            nodeRecords.close();
            propertyRecords.close();
            relRecords.close();
            schemaRuleChanges.close();
            relGroupRecords.close();
            propertyKeyTokenChanges.close();
            labelTokenChanges.close();
            relationshipTypeTokenChanges.close();
            changeCounter.clear();
        }
    }
}
