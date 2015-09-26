/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.kernel.impl.transaction.state.Loaders;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.state.RecordAccessSet;

public class DirectRecordAccessSet implements RecordAccessSet
{
    private final DirectRecordAccess<Long, NodeRecord, Void> nodeRecords;
    private final DirectRecordAccess<Long, PropertyRecord, PrimitiveRecord> propertyRecords;
    private final DirectRecordAccess<Long, RelationshipRecord, Void> relationshipRecords;
    private final DirectRecordAccess<Long, RelationshipGroupRecord, Integer> relationshipGroupRecords;
    private final DirectRecordAccess<Integer, PropertyKeyTokenRecord, Void> propertyKeyTokenRecords;
    private final DirectRecordAccess<Integer, RelationshipTypeTokenRecord, Void> relationshipTypeTokenRecords;
    private final DirectRecordAccess<Integer, LabelTokenRecord, Void> labelTokenRecords;
//    private final DirectRecordAccess<Long, Collection<DynamicRecord>, SchemaRule> schemaRecords; // TODO

    public DirectRecordAccessSet( NeoStore neoStore )
    {
        nodeRecords = new DirectRecordAccess<>( neoStore.getNodeStore(), Loaders.nodeLoader( neoStore.getNodeStore() ) );
        propertyRecords = new DirectRecordAccess<>( neoStore.getPropertyStore(), Loaders.propertyLoader( neoStore.getPropertyStore() ) );
        relationshipRecords = new DirectRecordAccess<>( neoStore.getRelationshipStore(), Loaders.relationshipLoader( neoStore.getRelationshipStore() ) );
        relationshipGroupRecords = new DirectRecordAccess<>( neoStore.getRelationshipGroupStore(), Loaders.relationshipGroupLoader( neoStore.getRelationshipGroupStore() ) );
        propertyKeyTokenRecords = new DirectRecordAccess<>( neoStore.getPropertyKeyTokenStore(), Loaders.propertyKeyTokenLoader( neoStore.getPropertyKeyTokenStore() ) );
        relationshipTypeTokenRecords = new DirectRecordAccess<>( neoStore.getRelationshipTypeTokenStore(), Loaders.relationshipTypeTokenLoader( neoStore.getRelationshipTypeTokenStore() ) );
        labelTokenRecords = new DirectRecordAccess<>( neoStore.getLabelTokenStore(), Loaders.labelTokenLoader( neoStore.getLabelTokenStore() ) );
//        schemaRecords = new DirectRecordAccess<>( neoStore.getSchemaStore(), Loaders.schemaRuleLoader( neoStore ) ); // TODO
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
    public RecordAccess<Long, Collection<DynamicRecord>, SchemaRule> getSchemaRuleChanges()
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
        nodeRecords.close();
        propertyRecords.close();
        relationshipRecords.close();
        relationshipGroupRecords.close();
//        schemaRecords.close(); // TODO
        relationshipTypeTokenRecords.close();
        labelTokenRecords.close();
        propertyKeyTokenRecords.close();
    }

    public void commit()
    {
        nodeRecords.commit();
        propertyRecords.commit();
        relationshipGroupRecords.commit();
        relationshipRecords.commit();
//        schemaRecords.commit(); // TODO
        relationshipTypeTokenRecords.commit();
        labelTokenRecords.commit();
        propertyKeyTokenRecords.commit();
    }

    @Override
    public boolean hasChanges()
    {
        return  nodeRecords.changeSize() > 0 ||
                propertyRecords.changeSize() > 0 ||
                relationshipRecords.changeSize() > 0 ||
                relationshipGroupRecords.changeSize() > 0 ||
                propertyKeyTokenRecords.changeSize() > 0 ||
                labelTokenRecords.changeSize() > 0 ||
                relationshipTypeTokenRecords.changeSize() > 0;
    }
}
