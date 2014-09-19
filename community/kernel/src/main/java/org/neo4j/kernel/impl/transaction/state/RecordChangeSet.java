/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.Collection;

import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;

public class RecordChangeSet implements RecordAccessSet
{
    private final RecordChanges<Long, NodeRecord, Void> nodeRecords;
    private final RecordChanges<Long, PropertyRecord, PrimitiveRecord> propertyRecords;
    private final RecordChanges<Long, RelationshipRecord, Void> relRecords;
    private final RecordChanges<Long, RelationshipGroupRecord, Integer> relGroupRecords;
    private final RecordChanges<Long, Collection<DynamicRecord>, SchemaRule> schemaRuleChanges;
    private final RecordChanges<Integer, PropertyKeyTokenRecord, Void> propertyKeyTokenChanges;
    private final RecordChanges<Integer, LabelTokenRecord, Void> labelTokenChanges;
    private final RecordChanges<Integer, RelationshipTypeTokenRecord, Void> relationshipTypeTokenChanges;

    public RecordChangeSet( NeoStore neoStore )
    {
        this.nodeRecords = new RecordChanges<>( Loaders.nodeLoader( neoStore.getNodeStore() ), true );
        this.propertyRecords = new RecordChanges<>( Loaders.propertyLoader( neoStore.getPropertyStore() ), true );
        this.relRecords = new RecordChanges<>( Loaders.relationshipLoader( neoStore.getRelationshipStore() ), false );
        this.relGroupRecords = new RecordChanges<>( Loaders.relationshipGroupLoader( neoStore.getRelationshipGroupStore() ), false );
        this.schemaRuleChanges = new RecordChanges<>( Loaders.schemaRuleLoader( neoStore.getSchemaStore() ), true );
        this.propertyKeyTokenChanges = new RecordChanges<>(
                Loaders.propertyKeyTokenLoader( neoStore.getPropertyKeyTokenStore() ), false );
        this.labelTokenChanges = new RecordChanges<>(
                Loaders.labelTokenLoader( neoStore.getLabelTokenStore() ), false );
        this.relationshipTypeTokenChanges = new RecordChanges<>(
                Loaders.relationshipTypeTokenLoader( neoStore.getRelationshipTypeTokenStore() ), false );
    }

    @Override
    public RecordChanges<Long, NodeRecord, Void> getNodeRecords()
    {
        return nodeRecords;
    }

    @Override
    public RecordChanges<Long, PropertyRecord, PrimitiveRecord> getPropertyRecords()
    {
        return propertyRecords;
    }

    @Override
    public RecordChanges<Long, RelationshipRecord, Void> getRelRecords()
    {
        return relRecords;
    }

    @Override
    public RecordChanges<Long, RelationshipGroupRecord, Integer> getRelGroupRecords()
    {
        return relGroupRecords;
    }

    @Override
    public RecordChanges<Long, Collection<DynamicRecord>, SchemaRule> getSchemaRuleChanges()
    {
        return schemaRuleChanges;
    }

    @Override
    public RecordChanges<Integer, PropertyKeyTokenRecord, Void> getPropertyKeyTokenChanges()
    {
        return propertyKeyTokenChanges;
    }

    @Override
    public RecordChanges<Integer, LabelTokenRecord, Void> getLabelTokenChanges()
    {
        return labelTokenChanges;
    }

    @Override
    public RecordChanges<Integer, RelationshipTypeTokenRecord, Void> getRelationshipTypeTokenChanges()
    {
        return relationshipTypeTokenChanges;
    }

    @Override
    public void close()
    {
        nodeRecords.close();
        propertyRecords.close();
        relRecords.close();
        schemaRuleChanges.close();
        relGroupRecords.close();
        propertyKeyTokenChanges.close();
        labelTokenChanges.close();
        relationshipTypeTokenChanges.close();
    }
}
