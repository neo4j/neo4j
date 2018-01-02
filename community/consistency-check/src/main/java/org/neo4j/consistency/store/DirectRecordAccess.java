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
package org.neo4j.consistency.store;

import java.util.Iterator;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public class DirectRecordAccess implements RecordAccess
{
    final StoreAccess access;
    final CacheAccess cacheAccess;

    public DirectRecordAccess( StoreAccess access, CacheAccess cacheAccess )
    {
        this.access = access;
        this.cacheAccess = cacheAccess;
    }

    @Override
    public RecordReference<DynamicRecord> schema( long id )
    {
        return referenceTo( access.getSchemaStore(), id );
    }

    @Override
    public RecordReference<NodeRecord> node( long id )
    {
        return referenceTo( access.getNodeStore(), id );
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id )
    {
        return referenceTo( access.getRelationshipStore(), id );
    }

    @Override
    public RecordReference<RelationshipGroupRecord> relationshipGroup( long id )
    {
        return referenceTo( access.getRelationshipGroupStore(), id );
    }

    @Override
    public RecordReference<PropertyRecord> property( long id )
    {
        return referenceTo( access.getPropertyStore(), id );
    }

    @Override
    public Iterator<PropertyRecord> rawPropertyChain( final long firstId )
    {
        return new PrefetchingIterator<PropertyRecord>()
        {
            private long next = firstId;

            @Override
            protected PropertyRecord fetchNextOrNull()
            {
                if ( Record.NO_NEXT_PROPERTY.is( next ) )
                {
                    return null;
                }

                PropertyRecord record = referenceTo( access.getPropertyStore(), next ).record();
                next = record.getNextProp();
                return record;
            }
        };
    }

    @Override
    public RecordReference<RelationshipTypeTokenRecord> relationshipType( int id )
    {
        return referenceTo( access.getRelationshipTypeTokenStore(), id );
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id )
    {
        return referenceTo( access.getPropertyKeyTokenStore(), id );
    }

    @Override
    public RecordReference<DynamicRecord> string( long id )
    {
        return referenceTo( access.getStringStore(), id );
    }

    @Override
    public RecordReference<DynamicRecord> array( long id )
    {
        return referenceTo( access.getArrayStore(), id );
    }

    @Override
    public RecordReference<DynamicRecord> relationshipTypeName( int id )
    {
        return referenceTo( access.getRelationshipTypeNameStore(), id );
    }

    @Override
    public RecordReference<DynamicRecord> nodeLabels( long id )
    {
        return referenceTo( access.getNodeDynamicLabelStore(), id );
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id )
    {
        return referenceTo( access.getLabelTokenStore(), id );
    }

    @Override
    public RecordReference<DynamicRecord> labelName( int id )
    {
        return referenceTo( access.getLabelNameStore(), id );
    }

    @Override
    public RecordReference<DynamicRecord> propertyKeyName( int id )
    {
        return referenceTo( access.getPropertyKeyNameStore(), id );
    }


    @Override
    public RecordReference<NeoStoreRecord> graph()
    {
        return new DirectRecordReference<>( access.getRawNeoStores().getMetaDataStore().asRecord(), this );
    }

    @Override
    public boolean shouldCheck( long id, MultiPassStore store )
    {
        return true;
    }

    <RECORD extends AbstractBaseRecord> DirectRecordReference<RECORD> referenceTo( RecordStore<RECORD> store, long id )
    {
        return new DirectRecordReference<>( store.forceGetRecord( id ), this);
    }

    @Override
    public CacheAccess cacheAccess()
    {
        return cacheAccess;
    }
}
