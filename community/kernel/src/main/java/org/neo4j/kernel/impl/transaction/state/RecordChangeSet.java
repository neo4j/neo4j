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
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.util.statistics.IntCounter;

public class RecordChangeSet implements RecordAccessSet
{
    private final RecordChanges<Long, NodeRecord, Void> nodeChanges;
    private final RecordChanges<Long, PropertyRecord, PrimitiveRecord> propertyChanges;
    private final RecordChanges<Long, RelationshipRecord, Void> relationshipChanges;
    private final RecordChanges<Long, RelationshipGroupRecord, Integer> relationshipGroupChanges;
    private final RecordChanges<Long, Collection<DynamicRecord>, SchemaRule> schemaRuleChanges;
    private final RecordChanges<Integer, PropertyKeyTokenRecord, Void> propertyKeyTokenChanges;
    private final RecordChanges<Integer, LabelTokenRecord, Void> labelTokenChanges;
    private final RecordChanges<Integer, RelationshipTypeTokenRecord, Void> relationshipTypeTokenChanges;
    private final RecordChanges<Long, NeoStoreRecord, Void> neoStoreChanges;
    private final IntCounter changeCounter = new IntCounter();

    public RecordChangeSet( NeoStore neoStore )
    {
        this.nodeChanges = new RecordChanges<>(
                Loaders.nodeLoader( neoStore.getNodeStore() ), true, changeCounter );
        this.propertyChanges = new RecordChanges<>(
                Loaders.propertyLoader( neoStore.getPropertyStore() ), true, changeCounter );
        this.relationshipChanges = new RecordChanges<>(
                Loaders.relationshipLoader( neoStore.getRelationshipStore() ), false, changeCounter );
        this.relationshipGroupChanges = new RecordChanges<>(
                Loaders.relationshipGroupLoader( neoStore.getRelationshipGroupStore() ), false, changeCounter );
        this.schemaRuleChanges = new RecordChanges<>(
                Loaders.schemaRuleLoader( neoStore.getSchemaStore() ), true, changeCounter );
        this.propertyKeyTokenChanges = new RecordChanges<>(
                Loaders.propertyKeyTokenLoader( neoStore.getPropertyKeyTokenStore() ), false, changeCounter );
        this.labelTokenChanges = new RecordChanges<>(
                Loaders.labelTokenLoader( neoStore.getLabelTokenStore() ), false, changeCounter );
        this.relationshipTypeTokenChanges = new RecordChanges<>(
                Loaders.relationshipTypeTokenLoader( neoStore.getRelationshipTypeTokenStore() ), false, changeCounter );
        this.neoStoreChanges = new RecordChanges<>(
                Loaders.neoStoreLoader( neoStore ), false, changeCounter );
    }

    @Override
    public RecordChanges<Long, NodeRecord, Void> getNodeChanges()
    {
        return nodeChanges;
    }

    @Override
    public RecordChanges<Long, PropertyRecord, PrimitiveRecord> getPropertyChanges()
    {
        return propertyChanges;
    }

    @Override
    public RecordChanges<Long, RelationshipRecord, Void> getRelationshipChanges()
    {
        return relationshipChanges;
    }

    @Override
    public RecordChanges<Long, RelationshipGroupRecord, Integer> getRelationshipGroupChanges()
    {
        return relationshipGroupChanges;
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
    public RecordChanges<Long, NeoStoreRecord, Void> getNeoStoreChanges()
    {
        return neoStoreChanges;
    }

    @Override
    public boolean hasChanges()
    {
        return changeCounter.value() > 0;
    }

    @Override
    public void close()
    {
        if ( hasChanges() )
        {
            nodeChanges.close();
            propertyChanges.close();
            relationshipChanges.close();
            schemaRuleChanges.close();
            relationshipGroupChanges.close();
            propertyKeyTokenChanges.close();
            labelTokenChanges.close();
            relationshipTypeTokenChanges.close();
            changeCounter.clear();
        }
    }
}
