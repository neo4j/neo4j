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
package org.neo4j.kernel.impl.store;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Container for {@link RecordCursor}s for different stores. Intended to be reused by pooled transactions.
 */
public class RecordCursors implements AutoCloseable
{
    private final RecordCursor<NodeRecord> node;
    private final RecordCursor<RelationshipRecord> relationship;
    private final RecordCursor<RelationshipGroupRecord> relationshipGroup;
    private final RecordCursor<PropertyRecord> property;
    private final RecordCursor<DynamicRecord> propertyString;
    private final RecordCursor<DynamicRecord> propertyArray;
    private final RecordCursor<DynamicRecord> label;

    public RecordCursors( NeoStores neoStores )
    {
        node = newCursor( neoStores.getNodeStore() );
        relationship = newCursor( neoStores.getRelationshipStore() );
        relationshipGroup = newCursor( neoStores.getRelationshipGroupStore() );
        property = newCursor( neoStores.getPropertyStore() );
        propertyString = newCursor( neoStores.getPropertyStore().getStringStore() );
        propertyArray = newCursor( neoStores.getPropertyStore().getArrayStore() );
        label = newCursor( neoStores.getNodeStore().getDynamicLabelStore() );
    }

    private static <R extends AbstractBaseRecord> RecordCursor<R> newCursor( RecordStore<R> store )
    {
        return store.newRecordCursor( store.newRecord() ).acquire( store.getNumberOfReservedLowIds(), NORMAL );
    }

    @Override
    public void close()
    {
        IOUtils.closeAll( RuntimeException.class,
                node, relationship, relationshipGroup, property, propertyArray, propertyString, label );
    }

    public RecordCursor<NodeRecord> node()
    {
        return node;
    }

    public RecordCursor<RelationshipRecord> relationship()
    {
        return relationship;
    }

    public RecordCursor<RelationshipGroupRecord> relationshipGroup()
    {
        return relationshipGroup;
    }

    public RecordCursor<PropertyRecord> property()
    {
        return property;
    }

    public RecordCursor<DynamicRecord> propertyArray()
    {
        return propertyArray;
    }

    public RecordCursor<DynamicRecord> propertyString()
    {
        return propertyString;
    }

    public RecordCursor<DynamicRecord> label()
    {
        return label;
    }
}
