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
package org.neo4j.legacy.consistency.store;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public class DelegatingRecordAccess implements DiffRecordAccess
{
    private final DiffRecordAccess delegate;

    public DelegatingRecordAccess( DiffRecordAccess delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public RecordReference<NodeRecord> previousNode( long id )
    {
        return delegate.previousNode( id );
    }

    @Override
    public RecordReference<RelationshipRecord> previousRelationship( long id )
    {
        return delegate.previousRelationship( id );
    }

    @Override
    public RecordReference<PropertyRecord> previousProperty( long id )
    {
        return delegate.previousProperty( id );
    }

    @Override
    public RecordReference<NeoStoreRecord> previousGraph()
    {
        return delegate.previousGraph();
    }

    @Override
    public DynamicRecord changedSchema( long id )
    {
        return delegate.changedSchema( id );
    }

    @Override
    public NodeRecord changedNode( long id )
    {
        return delegate.changedNode( id );
    }

    @Override
    public RelationshipRecord changedRelationship( long id )
    {
        return delegate.changedRelationship( id );
    }

    @Override
    public PropertyRecord changedProperty( long id )
    {
        return delegate.changedProperty( id );
    }

    @Override
    public DynamicRecord changedString( long id )
    {
        return delegate.changedString( id );
    }

    @Override
    public DynamicRecord changedArray( long id )
    {
        return delegate.changedArray( id );
    }

    @Override
    public RecordReference<DynamicRecord> schema( long id )
    {
        return delegate.schema( id );
    }

    @Override
    public RecordReference<NodeRecord> node( long id )
    {
        return delegate.node( id );
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id )
    {
        return delegate.relationship( id );
    }

    @Override
    public RecordReference<PropertyRecord> property( long id )
    {
        return delegate.property( id );
    }

    @Override
    public RecordReference<RelationshipTypeTokenRecord> relationshipType( int id )
    {
        return delegate.relationshipType( id );
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id )
    {
        return delegate.propertyKey( id );
    }

    @Override
    public RecordReference<DynamicRecord> string( long id )
    {
        return delegate.string( id );
    }

    @Override
    public RecordReference<DynamicRecord> array( long id )
    {
        return delegate.array( id );
    }

    @Override
    public RecordReference<DynamicRecord> relationshipTypeName( int id )
    {
        return delegate.relationshipTypeName( id );
    }

    @Override
    public RecordReference<DynamicRecord> nodeLabels( long id )
    {
        return delegate.nodeLabels( id );
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id )
    {
        return delegate.label( id );
    }

    @Override
    public RecordReference<DynamicRecord> labelName( int id )
    {
        return delegate.labelName( id );
    }

    @Override
    public RecordReference<DynamicRecord> propertyKeyName( int id )
    {
        return delegate.propertyKeyName( id );
    }

    @Override
    public RecordReference<NeoStoreRecord> graph()
    {
        return delegate.graph();
    }

    @Override
    public RecordReference<RelationshipGroupRecord> relationshipGroup( long id )
    {
        return delegate.relationshipGroup( id );
    }

    @Override
    public RelationshipGroupRecord changedRelationshipGroup( long id )
    {
        return delegate.changedRelationshipGroup( id );
    }
}
