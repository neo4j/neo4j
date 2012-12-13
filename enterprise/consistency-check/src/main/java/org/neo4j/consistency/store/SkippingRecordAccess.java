/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.store;

import static org.neo4j.consistency.store.RecordReference.SkippingReference.skipReference;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

@SuppressWarnings("unchecked")
public class SkippingRecordAccess implements DiffRecordAccess
{
    @Override
    public RecordReference<NodeRecord> node( long id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<PropertyRecord> property( long id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<RelationshipTypeRecord> relationshipLabel( int id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<PropertyIndexRecord> propertyKey( int id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> string( long id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> array( long id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> relationshipLabelName( int id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> propertyKeyName( int id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<NeoStoreRecord> graph()
    {
        return skipReference();
    }

    @Override
    public RecordReference<NodeRecord> previousNode( long id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<RelationshipRecord> previousRelationship( long id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<PropertyRecord> previousProperty( long id )
    {
        return skipReference();
    }

    @Override
    public RecordReference<NeoStoreRecord> previousGraph()
    {
        return skipReference();
    }

    @Override
    public NodeRecord changedNode( long id )
    {
        return null;
    }

    @Override
    public RelationshipRecord changedRelationship( long id )
    {
        return null;
    }

    @Override
    public PropertyRecord changedProperty( long id )
    {
        return null;
    }

    @Override
    public DynamicRecord changedString( long id )
    {
        return null;
    }

    @Override
    public DynamicRecord changedArray( long id )
    {
        return null;
    }
}
