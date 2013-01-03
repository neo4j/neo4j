/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class CacheSmallStoresRecordAccess implements DiffRecordAccess
{
    private final DiffRecordAccess delegate;
    private final PropertyIndexRecord[] propertyRecords;
    private final RelationshipTypeRecord[] relationshipLabels;

    public CacheSmallStoresRecordAccess( DiffRecordAccess delegate,
                                         PropertyIndexRecord[] propertyRecords,
                                         RelationshipTypeRecord[] relationshipLabels )
    {
        this.delegate = delegate;
        this.propertyRecords = propertyRecords;
        this.relationshipLabels = relationshipLabels;
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
    public RecordReference<RelationshipTypeRecord> relationshipLabel( int id )
    {
        if ( id < relationshipLabels.length )
        {
            return new DirectRecordReference<RelationshipTypeRecord>( relationshipLabels[id], this );
        }
        else
        {
            return delegate.relationshipLabel( id );
        }
    }

    @Override
    public RecordReference<PropertyIndexRecord> propertyKey( int id )
    {
        if ( id < propertyRecords.length )
        {
            return new DirectRecordReference<PropertyIndexRecord>( propertyRecords[id], this );
        }
        else
        {
            return delegate.propertyKey( id );
        }
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
    public RecordReference<DynamicRecord> relationshipLabelName( int id )
    {
        return delegate.relationshipLabelName( id );
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
}
