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
package org.neo4j.kernel.impl.api.state;

import java.util.Map;

import org.neo4j.collection.primitive.versioned.VersionedPrimitiveLongObjectMap;
import org.neo4j.kernel.impl.util.VersionedHashMap;
import org.neo4j.kernel.impl.util.diffsets.EmptyPrimitiveLongReadableDiffSets;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveLongDiffSets;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;

/**
 * Indexes entities by what property and value has been modified on them.
 */
public class PropertyChanges
{
    private VersionedPrimitiveLongObjectMap<Map<Object, PrimitiveLongDiffSets>> changes;

    PrimitiveLongReadableDiffSets changesForProperty( int propertyKeyId, Object value )
    {
        if ( changes != null )
        {
            Map<Object, PrimitiveLongDiffSets> keyChanges = changes.currentView().get( propertyKeyId );
            if ( keyChanges != null )
            {
                PrimitiveLongDiffSets valueChanges = keyChanges.get( value );
                if ( valueChanges != null )
                {
                    return valueChanges;
                }
            }
        }
        return EmptyPrimitiveLongReadableDiffSets.INSTANCE;
    }

    void changeProperty( long entityId, int propertyKeyId, Object oldValue, Object newValue )
    {
        Map<Object, PrimitiveLongDiffSets> keyChanges = keyChanges( propertyKeyId );
        valueChanges( newValue, keyChanges ).add( entityId );
        valueChanges( oldValue, keyChanges ).remove( entityId );
    }

    void addProperty( long entityId, int propertyKeyId, Object value )
    {
        valueChanges( value, keyChanges( propertyKeyId ) ).add( entityId );
    }

    public void removeProperty( long entityId, int propertyKeyId, Object oldValue )
    {
        valueChanges( oldValue, keyChanges( propertyKeyId ) ).remove( entityId );
    }

    private Map<Object, PrimitiveLongDiffSets> keyChanges( int propertyKeyId )
    {
        if ( changes == null )
        {
            changes = new VersionedPrimitiveLongObjectMap<>();
        }

        return changes.currentView().computeIfAbsent( propertyKeyId, k -> new VersionedHashMap<>() );
    }

    private PrimitiveLongDiffSets valueChanges( Object newValue, Map<Object, PrimitiveLongDiffSets> keyChanges )
    {
        return keyChanges.computeIfAbsent( newValue, k -> new PrimitiveLongDiffSets() );
    }
}
