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
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

public class DelegatingRecordAccess implements RecordAccess
{
    private final RecordAccess delegate;

    public DelegatingRecordAccess( RecordAccess delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public RecordReference<SchemaRecord> schema( long id, PageCursorTracer cursorTracer )
    {
        return delegate.schema( id, cursorTracer );
    }

    @Override
    public RecordReference<NodeRecord> node( long id, PageCursorTracer cursorTracer )
    {
        return delegate.node( id, cursorTracer );
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id, PageCursorTracer cursorTracer )
    {
        return delegate.relationship( id, cursorTracer );
    }

    @Override
    public RecordReference<PropertyRecord> property( long id, PageCursorTracer cursorTracer )
    {
        return delegate.property( id, cursorTracer );
    }

    @Override
    public Iterator<PropertyRecord> rawPropertyChain( long firstId, PageCursorTracer cursorTracer )
    {
        return delegate.rawPropertyChain( firstId, cursorTracer );
    }

    @Override
    public RecordReference<RelationshipTypeTokenRecord> relationshipType( int id, PageCursorTracer cursorTracer )
    {
        return delegate.relationshipType( id, cursorTracer );
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id, PageCursorTracer cursorTracer )
    {
        return delegate.propertyKey( id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> string( long id, PageCursorTracer cursorTracer )
    {
        return delegate.string( id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> array( long id, PageCursorTracer cursorTracer )
    {
        return delegate.array( id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> relationshipTypeName( int id, PageCursorTracer cursorTracer )
    {
        return delegate.relationshipTypeName( id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> nodeLabels( long id, PageCursorTracer cursorTracer )
    {
        return delegate.nodeLabels( id, cursorTracer );
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id, PageCursorTracer cursorTracer )
    {
        return delegate.label( id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> labelName( int id, PageCursorTracer cursorTracer )
    {
        return delegate.labelName( id, cursorTracer );
    }

    @Override
    public RecordReference<DynamicRecord> propertyKeyName( int id, PageCursorTracer cursorTracer )
    {
        return delegate.propertyKeyName( id, cursorTracer );
    }

    @Override
    public RecordReference<RelationshipGroupRecord> relationshipGroup( long id, PageCursorTracer cursorTracer )
    {
        return delegate.relationshipGroup( id, cursorTracer );
    }

    @Override
    public boolean shouldCheck( long id, MultiPassStore store )
    {
        return delegate.shouldCheck( id, store );
    }

    @Override
    public CacheAccess cacheAccess()
    {
        return delegate.cacheAccess();
    }
}
