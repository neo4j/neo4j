/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.NeoStores;
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
import org.neo4j.kernel.impl.transaction.state.RecordAccess.Loader;
import org.neo4j.kernel.impl.util.statistics.IntCounter;

public class RecordChangeSet implements RecordAccessSet
{
    private final RecordAccess<Long, NodeRecord, Void> nodeRecords;
    private final RecordAccess<Long, PropertyRecord, PrimitiveRecord> propertyRecords;
    private final RecordAccess<Long, RelationshipRecord, Void> relRecords;
    private final RecordAccess<Long, RelationshipGroupRecord, Integer> relGroupRecords;
    private final RecordAccess<Long, Collection<DynamicRecord>, SchemaRule> schemaRuleChanges;
    private final RecordAccess<Integer, PropertyKeyTokenRecord, Void> propertyKeyTokenChanges;
    private final RecordAccess<Integer, LabelTokenRecord, Void> labelTokenChanges;
    private final RecordAccess<Integer, RelationshipTypeTokenRecord, Void> relationshipTypeTokenChanges;
    private final IntCounter changeCounter = new IntCounter();

    public RecordChangeSet( NeoStores neoStores )
    {
        this( false, neoStores );
    }

    public RecordChangeSet( boolean beforeStateForAll, NeoStores neoStores )
    {
        this(   beforeStateForAll,
                Loaders.nodeLoader( neoStores.getNodeStore() ),
                Loaders.propertyLoader( neoStores.getPropertyStore() ),
                Loaders.relationshipLoader( neoStores.getRelationshipStore() ),
                Loaders.relationshipGroupLoader( neoStores.getRelationshipGroupStore() ),
                Loaders.schemaRuleLoader( neoStores.getSchemaStore() ),
                Loaders.propertyKeyTokenLoader( neoStores.getPropertyKeyTokenStore() ),
                Loaders.labelTokenLoader( neoStores.getLabelTokenStore() ),
                Loaders.relationshipTypeTokenLoader( neoStores.getRelationshipTypeTokenStore() ) );
    }

    public RecordChangeSet(
            boolean beforeStateForAll,
            Loader<Long,NodeRecord,Void> nodeLoader,
            Loader<Long,PropertyRecord,PrimitiveRecord> propertyLoader,
            Loader<Long,RelationshipRecord,Void> relationshipLoader,
            Loader<Long,RelationshipGroupRecord,Integer> relationshipGroupLoader,
            Loader<Long,Collection<DynamicRecord>,SchemaRule> schemaRuleLoader,
            Loader<Integer,PropertyKeyTokenRecord,Void> propertyKeyTokenLoader,
            Loader<Integer,LabelTokenRecord,Void> labelTokenLoader,
            Loader<Integer,RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader )
    {
        this.nodeRecords = new RecordChanges<>( nodeLoader, true, changeCounter );
        this.propertyRecords = new RecordChanges<>( propertyLoader, true, changeCounter );
        this.relRecords = new RecordChanges<>( relationshipLoader, beforeStateForAll, changeCounter );
        this.relGroupRecords = new RecordChanges<>( relationshipGroupLoader, beforeStateForAll, changeCounter );
        this.schemaRuleChanges = new RecordChanges<>( schemaRuleLoader, true, changeCounter );
        this.propertyKeyTokenChanges = new RecordChanges<>( propertyKeyTokenLoader, beforeStateForAll, changeCounter );
        this.labelTokenChanges = new RecordChanges<>( labelTokenLoader, beforeStateForAll, changeCounter );
        this.relationshipTypeTokenChanges = new RecordChanges<>( relationshipTypeTokenLoader, beforeStateForAll, changeCounter );
    }

    @Override
    public RecordAccess<Long, NodeRecord, Void> getNodeRecords()
    {
        return nodeRecords;
    }

    @Override
    public RecordAccess<Long, PropertyRecord, PrimitiveRecord> getPropertyRecords()
    {
        return propertyRecords;
    }

    @Override
    public RecordAccess<Long, RelationshipRecord, Void> getRelRecords()
    {
        return relRecords;
    }

    @Override
    public RecordAccess<Long, RelationshipGroupRecord, Integer> getRelGroupRecords()
    {
        return relGroupRecords;
    }

    @Override
    public RecordAccess<Long, Collection<DynamicRecord>, SchemaRule> getSchemaRuleChanges()
    {
        return schemaRuleChanges;
    }

    @Override
    public RecordAccess<Integer, PropertyKeyTokenRecord, Void> getPropertyKeyTokenChanges()
    {
        return propertyKeyTokenChanges;
    }

    @Override
    public RecordAccess<Integer, LabelTokenRecord, Void> getLabelTokenChanges()
    {
        return labelTokenChanges;
    }

    @Override
    public RecordAccess<Integer, RelationshipTypeTokenRecord, Void> getRelationshipTypeTokenChanges()
    {
        return relationshipTypeTokenChanges;
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
