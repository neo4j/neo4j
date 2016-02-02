/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.batchinsert;

import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.state.Loaders;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.state.RecordAccessSet;
import org.neo4j.storageengine.api.schema.SchemaRule;

public class DirectRecordAccessSet implements RecordAccessSet
{
    private final DirectRecordAccess<Long, NodeRecord, Void> nodeRecords;
    private final DirectRecordAccess<Long, PropertyRecord, PrimitiveRecord> propertyRecords;
    private final DirectRecordAccess<Long, RelationshipRecord, Void> relationshipRecords;
    private final DirectRecordAccess<Long, RelationshipGroupRecord, Integer> relationshipGroupRecords;
    private final DirectRecordAccess<Integer, PropertyKeyTokenRecord, Void> propertyKeyTokenRecords;
    private final DirectRecordAccess<Integer, RelationshipTypeTokenRecord, Void> relationshipTypeTokenRecords;
    private final DirectRecordAccess<Integer, LabelTokenRecord, Void> labelTokenRecords;
    private final DirectRecordAccess[] all;

    public DirectRecordAccessSet( NeoStores neoStores )
    {
        Loaders loaders = new Loaders( neoStores );
        NodeStore nodeStore = neoStores.getNodeStore();
        PropertyStore propertyStore = neoStores.getPropertyStore();
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
        PropertyKeyTokenStore propertyKeyTokenStore = neoStores.getPropertyKeyTokenStore();
        RelationshipTypeTokenStore relationshipTypeTokenStore = neoStores.getRelationshipTypeTokenStore();
        LabelTokenStore labelTokenStore = neoStores.getLabelTokenStore();
        nodeRecords = new DirectRecordAccess<>( nodeStore, loaders.nodeLoader() );
        propertyRecords = new DirectRecordAccess<>( propertyStore, loaders.propertyLoader() );
        relationshipRecords = new DirectRecordAccess<>( relationshipStore, loaders.relationshipLoader() );
        relationshipGroupRecords = new DirectRecordAccess<>(
                relationshipGroupStore, loaders.relationshipGroupLoader() );
        propertyKeyTokenRecords = new DirectRecordAccess<>( propertyKeyTokenStore, loaders.propertyKeyTokenLoader() );
        relationshipTypeTokenRecords = new DirectRecordAccess<>(
                relationshipTypeTokenStore, loaders.relationshipTypeTokenLoader() );
        labelTokenRecords = new DirectRecordAccess<>( labelTokenStore, loaders.labelTokenLoader() );
        all = new DirectRecordAccess[] {
                nodeRecords, propertyRecords, relationshipRecords, relationshipGroupRecords,
                propertyKeyTokenRecords, relationshipTypeTokenRecords, labelTokenRecords
        };
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
        return relationshipRecords;
    }

    @Override
    public RecordAccess<Long, RelationshipGroupRecord, Integer> getRelGroupRecords()
    {
        return relationshipGroupRecords;
    }

    @Override
    public RecordAccess<Long, SchemaRecord, SchemaRule> getSchemaRuleChanges()
    {
        throw new UnsupportedOperationException( "Not needed. Implement if needed" );
    }

    @Override
    public RecordAccess<Integer, PropertyKeyTokenRecord, Void> getPropertyKeyTokenChanges()
    {
        return propertyKeyTokenRecords;
    }

    @Override
    public RecordAccess<Integer, LabelTokenRecord, Void> getLabelTokenChanges()
    {
        return labelTokenRecords;
    }

    @Override
    public RecordAccess<Integer, RelationshipTypeTokenRecord, Void> getRelationshipTypeTokenChanges()
    {
        return relationshipTypeTokenRecords;
    }

    @Override
    public void close()
    {
        commit();
        for ( DirectRecordAccess access : all )
        {
            access.close();
        }
    }

    public void commit()
    {
        for ( DirectRecordAccess access : all )
        {
            access.commit();
        }
    }

    @Override
    public boolean hasChanges()
    {
        for ( DirectRecordAccess access : all )
        {
            if ( access.changeSize() > 0 )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public int changeSize()
    {
        int total = 0;
        for ( DirectRecordAccess access : all )
        {
            total += access.changeSize();
        }
        return total;
    }
}
