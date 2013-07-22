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
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;

public class DirectRecordAccess implements DiffRecordAccess
{
    private final StoreAccess access;

    public DirectRecordAccess( StoreAccess access )
    {
        this.access = access;
    }

    @Override
    public RecordReference<DynamicRecord> schema( long id )
    {
        return new DirectRecordReference<>( access.getSchemaStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<NodeRecord> node( long id )
    {
        return new DirectRecordReference<>( access.getNodeStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id )
    {
        return new DirectRecordReference<>( access.getRelationshipStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<PropertyRecord> property( long id )
    {
        return new DirectRecordReference<>( access.getPropertyStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<RelationshipTypeTokenRecord> relationshipType( int id )
    {
        return new DirectRecordReference<>( access.getRelationshipTypeTokenStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id )
    {
        return new DirectRecordReference<>( access.getPropertyKeyTokenStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<DynamicRecord> string( long id )
    {
        return new DirectRecordReference<>( access.getStringStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<DynamicRecord> array( long id )
    {
        return new DirectRecordReference<>( access.getArrayStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<DynamicRecord> relationshipTypeName( int id )
    {
        return new DirectRecordReference<>( access.getRelationshipTypeNameStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<DynamicRecord> nodeLabels( long id )
    {
        return new DirectRecordReference<>( access.getNodeDynamicLabelStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id )
    {
        return new DirectRecordReference<>( access.getLabelTokenStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<DynamicRecord> labelName( int id )
    {
        return new DirectRecordReference<>( access.getLabelNameStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<DynamicRecord> propertyKeyName( int id )
    {
        return new DirectRecordReference<>( access.getPropertyKeyNameStore().forceGetRecord( id ), this );
    }

    @Override
    public RecordReference<NeoStoreRecord> graph()
    {
        if ( access instanceof DiffStore )
        {
            return new DirectRecordReference<>( ((DiffStore) access).getMasterRecord(), this );
        }
        else
        {
            return new DirectRecordReference<>( access.getRawNeoStore().asRecord(), this );
        }
    }

    @Override
    public RecordReference<NodeRecord> previousNode( long id )
    {
        return new DirectRecordReference<>( access.getNodeStore().forceGetRaw( id ), this );
    }

    @Override
    public RecordReference<RelationshipRecord> previousRelationship( long id )
    {
        return new DirectRecordReference<>( access.getRelationshipStore().forceGetRaw( id ), this );
    }

    @Override
    public RecordReference<PropertyRecord> previousProperty( long id )
    {
        return new DirectRecordReference<>( access.getPropertyStore().forceGetRaw( id ), this );
    }

    @Override
    public DynamicRecord changedSchema( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getSchemaStore().getChangedRecord( id ) : null;
    }

    @Override
    public NodeRecord changedNode( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getNodeStore().getChangedRecord( id ) : null;
    }

    @Override
    public RelationshipRecord changedRelationship( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getRelationshipStore().getChangedRecord( id ) : null;
    }

    @Override
    public PropertyRecord changedProperty( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getPropertyStore().getChangedRecord( id ) : null;
    }

    @Override
    public DynamicRecord changedString( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getStringStore().getChangedRecord( id ) : null;
    }

    @Override
    public DynamicRecord changedArray( long id )
    {
        return access instanceof DiffStore ? ((DiffStore) access).getArrayStore().getChangedRecord( id ) : null;
    }

    @Override
    public RecordReference<NeoStoreRecord> previousGraph()
    {
        return new DirectRecordReference<>( access.getRawNeoStore().asRecord(), this );
    }
}
