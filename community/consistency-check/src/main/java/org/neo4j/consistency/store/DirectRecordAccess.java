/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.store;

import java.util.Iterator;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

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
    public RecordReference<SchemaRecord> schema( long id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getSchemaStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<NodeRecord> node( long id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getNodeStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getRelationshipStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<RelationshipGroupRecord> relationshipGroup( long id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getRelationshipGroupStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<PropertyRecord> property( long id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getPropertyStore(), id, cursorTracer );
    }

    @Override
    public Iterator<PropertyRecord> rawPropertyChain( final long firstId, PageCursorTracer cursorTracer )
    {
        return new PrefetchingIterator<>()
        {
            private long next = firstId;

            @Override
            protected PropertyRecord fetchNextOrNull()
            {
                if ( Record.NO_NEXT_PROPERTY.is( next ) )
                {
                    return null;
                }

                PropertyRecord record = referenceTo( access.getPropertyStore(), next, cursorTracer ).record();
                next = record.getNextProp();
                return record;
            }
        };
    }

    @Override
    public RecordReference<RelationshipTypeTokenRecord> relationshipType( int id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getRelationshipTypeTokenStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getPropertyKeyTokenStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> string( long id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getStringStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> array( long id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getArrayStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> relationshipTypeName( int id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getRelationshipTypeNameStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> nodeLabels( long id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getNodeDynamicLabelStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getLabelTokenStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> labelName( int id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getLabelNameStore(), id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> propertyKeyName( int id, PageCursorTracer cursorTracer )
    {
        return referenceTo( access.getPropertyKeyNameStore(), id, cursorTracer );
    }

    @Override
    public boolean shouldCheck( long id, MultiPassStore store )
    {
        return true;
    }

    <RECORD extends AbstractBaseRecord> DirectRecordReference<RECORD> referenceTo( RecordStore<RECORD> store, long id, PageCursorTracer cursorTracer )
    {
        return new DirectRecordReference<>( store.getRecord( id, store.newRecord(), FORCE, cursorTracer ), this, cursorTracer );
    }

    @Override
    public CacheAccess cacheAccess()
    {
        return cacheAccess;
    }
}
