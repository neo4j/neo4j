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

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterator;
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
    public RecordReference<NodeRecord> node( long id )
    {
        if ( shouldCheck( id, MultiPassStore.NODES ) )
        {
            return super.node( id );
        }
        return skipReference();
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id )
    {
        if ( shouldCheck( id, MultiPassStore.RELATIONSHIPS ) )
        {
            return super.relationship( id );
        }
        return skipReference();
    }

    @Override
    public RecordReference<RelationshipGroupRecord> relationshipGroup( long id )
    {
        if ( shouldCheck( id, MultiPassStore.RELATIONSHIP_GROUPS ) )
        {
            return super.relationshipGroup( id );
        }
        return skipReference();
    }

    @Override
    public RecordReference<PropertyRecord> property( long id )
    {
        if ( shouldCheck( id, MultiPassStore.PROPERTIES ) )
        {
            return super.property( id );
        }
        return skipReference();
    }

    @Override
    public Iterator<PropertyRecord> rawPropertyChain( long firstId )
    {
        return new FilteringIterator<>( super.rawPropertyChain( firstId ), new Predicate<PropertyRecord>()
        {
            @Override
            public boolean accept( PropertyRecord item )
            {
                return shouldCheck( item.getId() /*for some reason we don't care about the id*/,
                        MultiPassStore.PROPERTIES );
            }
        } );
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id )
    {
        if ( shouldCheck( id, MultiPassStore.PROPERTY_KEYS ) )
        {
            return super.propertyKey( id );
        }
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> string( long id )
    {
        if ( shouldCheck( id, MultiPassStore.STRINGS ) )
        {
            return super.string( id );
        }
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> array( long id )
    {
        if ( shouldCheck( id, MultiPassStore.ARRAYS ) )
        {
            return super.array( id );
        }
        return skipReference();
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id )
    {
        if ( shouldCheck( id, MultiPassStore.LABELS ) )
        {
            return super.label( id );
        }
        return skipReference();
    }

    @Override
    public RecordReference<DynamicRecord> nodeLabels( long id )
    {
        if ( shouldCheck( id, MultiPassStore.LABELS ) )
        {
            return super.nodeLabels( id );
        }
        return skipReference();
    }

    @Override
    public boolean shouldCheck( long id, MultiPassStore store )
    {
        return !(potentiallySkippableStores.contains( store ) && (currentStore != store));
    }
}
