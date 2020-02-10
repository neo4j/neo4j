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

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.internal.helpers.collection.FilteringIterator;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static java.util.Arrays.asList;
import static org.neo4j.consistency.store.RecordReference.SkippingReference.skipReference;

public class FilteringRecordAccess extends DelegatingRecordAccess
{
    private final Set<MultiPassStore> potentiallySkippableStores = EnumSet.noneOf( MultiPassStore.class );
    private final MultiPassStore currentStore;

    public FilteringRecordAccess( RecordAccess delegate,
            MultiPassStore currentStore, MultiPassStore... potentiallySkippableStores )
    {
        super( delegate );
        this.currentStore = currentStore;
        this.potentiallySkippableStores.addAll( asList( potentiallySkippableStores ) );
    }

    enum Mode
    {
        SKIP, FILTER
    }

    @Override
    public RecordReference<NodeRecord> node( long id, PageCursorTracer cursorTracer )
    {
        if ( shouldCheck( id, MultiPassStore.NODES ) )
        {
            return super.node( id, cursorTracer );
        }
        return skipReference();
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id, PageCursorTracer cursorTracer )
    {
        if ( shouldCheck( id, MultiPassStore.RELATIONSHIPS ) )
        {
            return super.relationship( id, cursorTracer );
        }
        return skipReference();
    }

    @Override
    public RecordReference<RelationshipGroupRecord> relationshipGroup( long id, PageCursorTracer cursorTracer )
    {
        if ( shouldCheck( id, MultiPassStore.RELATIONSHIP_GROUPS ) )
        {
            return super.relationshipGroup( id, cursorTracer );
        }
        return skipReference();
    }

    @Override
    public RecordReference<PropertyRecord> property( long id, PageCursorTracer cursorTracer )
    {
        if ( shouldCheck( id, MultiPassStore.PROPERTIES ) )
        {
            return super.property( id, cursorTracer );
        }
        return skipReference();
    }

    @Override
    public Iterator<PropertyRecord> rawPropertyChain( long firstId, PageCursorTracer cursorTracer )
    {
        return new FilteringIterator<>( super.rawPropertyChain( firstId, cursorTracer ),
                item -> shouldCheck( item.getId() /*for some reason we don't care about the id*/,
                        MultiPassStore.PROPERTIES ) );
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id, PageCursorTracer cursorTracer )
    {
        if ( shouldCheck( id, MultiPassStore.PROPERTY_KEYS ) )
        {
            return super.propertyKey( id, cursorTracer );
        }
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> string( long id, PageCursorTracer cursorTracer )
    {
        if ( shouldCheck( id, MultiPassStore.STRINGS ) )
        {
            return super.string( id, cursorTracer );
        }
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> array( long id, PageCursorTracer cursorTracer )
    {
        if ( shouldCheck( id, MultiPassStore.ARRAYS ) )
        {
            return super.array( id, cursorTracer );
        }
        return skipReference();
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id, PageCursorTracer cursorTracer )
    {
        if ( shouldCheck( id, MultiPassStore.LABELS ) )
        {
            return super.label( id, cursorTracer );
        }
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> nodeLabels( long id, PageCursorTracer cursorTracer )
    {
        if ( shouldCheck( id, MultiPassStore.LABELS ) )
        {
            return super.nodeLabels( id, cursorTracer );
        }
        return skipReference();
    }

    @Override
    public boolean shouldCheck( long id, MultiPassStore store )
    {
        return !(potentiallySkippableStores.contains( store ) && (currentStore != store));
    }
}
