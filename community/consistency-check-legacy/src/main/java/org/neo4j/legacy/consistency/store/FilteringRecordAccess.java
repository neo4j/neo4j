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

import java.util.EnumSet;
import java.util.Set;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.legacy.consistency.checking.full.MultiPassStore;

import static java.util.Arrays.asList;

import static org.neo4j.legacy.consistency.checking.full.MultiPassStore.recordInCurrentPass;
import static org.neo4j.legacy.consistency.store.RecordReference.SkippingReference.skipReference;

public class FilteringRecordAccess extends DelegatingRecordAccess
{
    private final Set<MultiPassStore> potentiallySkippableStores = EnumSet.noneOf( MultiPassStore.class );
    private final int iPass;
    private final long recordsPerPass;
    private final MultiPassStore currentStore;

    public FilteringRecordAccess( DiffRecordAccess delegate, final int iPass,
                                  final long recordsPerPass, MultiPassStore currentStore,
                                  MultiPassStore... potentiallySkippableStores )
    {
        super( delegate );
        this.iPass = iPass;
        this.recordsPerPass = recordsPerPass;
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
        if ( shouldSkip( id, MultiPassStore.NODES ) )
        {
            return skipReference();
        }
        return super.node( id );
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id )
    {
        if ( shouldSkip( id, MultiPassStore.RELATIONSHIPS ) )
        {
            return skipReference();
        }
        return super.relationship( id );
    }

    @Override
    public RecordReference<RelationshipGroupRecord> relationshipGroup( long id )
    {
        if ( shouldSkip( id, MultiPassStore.RELATIONSHIP_GROUPS ) )
        {
            return skipReference();
        }
        return super.relationshipGroup( id );
    }

    @Override
    public RecordReference<PropertyRecord> property( long id )
    {
        if ( shouldSkip( id, MultiPassStore.PROPERTIES ) )
        {
            return skipReference();
        }
        return super.property( id );
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id )
    {
        if ( shouldSkip( id, MultiPassStore.PROPERTY_KEYS ) )
        {
            return skipReference();
        }
        return super.propertyKey( id );
    }

    @Override
    public RecordReference<DynamicRecord> string( long id )
    {
        if ( shouldSkip( id, MultiPassStore.STRINGS ) )
        {
            return skipReference();
        }
        return super.string( id );
    }

    @Override
    public RecordReference<DynamicRecord> array( long id )
    {
        if ( shouldSkip( id, MultiPassStore.ARRAYS ) )
        {
            return skipReference();
        }
        return super.array( id );
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id )
    {
        if ( shouldSkip( id, MultiPassStore.LABELS ) )
        {
            return skipReference();
        }
        return super.label( id );
    }

    @Override
    public RecordReference<DynamicRecord> nodeLabels( long id )
    {
        if ( shouldSkip( id, MultiPassStore.LABELS ) )
        {
            return skipReference();
        }
        return super.nodeLabels( id );
    }

    private boolean shouldSkip( long id, MultiPassStore store )
    {
        return potentiallySkippableStores.contains( store ) &&
                (!isCurrentStore( store ) || !recordInCurrentPass( id, iPass, recordsPerPass ));
    }

    private boolean isCurrentStore( MultiPassStore store )
    {
        return currentStore == store;
    }
}
