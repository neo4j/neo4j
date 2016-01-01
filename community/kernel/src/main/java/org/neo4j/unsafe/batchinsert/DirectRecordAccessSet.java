/**
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

import java.util.Collection;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.xa.Loaders;
import org.neo4j.kernel.impl.nioneo.xa.RecordAccess;
import org.neo4j.kernel.impl.nioneo.xa.RecordAccessSet;

public class DirectRecordAccessSet implements RecordAccessSet
{
    private final DirectRecordAccess<Long, NodeRecord, Void> nodeRecords;
    private final DirectRecordAccess<Long, PropertyRecord, PrimitiveRecord> propertyRecords;
    private final DirectRecordAccess<Long, RelationshipRecord, Void> relationshipRecords;
    private final DirectRecordAccess<Long, RelationshipGroupRecord, Integer> relationshipGroupRecords;
//    private final DirectRecordAccess<Long, Collection<DynamicRecord>, SchemaRule> schemaRecords; // TODO

    public DirectRecordAccessSet( NeoStore neoStore )
    {
        nodeRecords = new DirectRecordAccess<>( neoStore.getNodeStore(), Loaders.nodeLoader( neoStore.getNodeStore() ) );
        propertyRecords = new DirectRecordAccess<>( neoStore.getPropertyStore(), Loaders.propertyLoader( neoStore.getPropertyStore() ) );
        relationshipRecords = new DirectRecordAccess<>( neoStore.getRelationshipStore(), Loaders.relationshipLoader( neoStore.getRelationshipStore() ) );
        relationshipGroupRecords = new DirectRecordAccess<>( neoStore.getRelationshipGroupStore(), Loaders.relationshipGroupLoader( neoStore.getRelationshipGroupStore() ) );
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
        throw new UnsupportedOperationException();
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
    }

    public void commit()
    {
        nodeRecords.commit();
        propertyRecords.commit();
        relationshipGroupRecords.commit();
        relationshipRecords.commit();
//        schemaRecords.commit(); // TODO
    }
}
