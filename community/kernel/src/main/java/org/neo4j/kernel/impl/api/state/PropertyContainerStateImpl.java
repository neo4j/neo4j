/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.Iterator;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;

class PropertyContainerStateImpl implements PropertyContainerState
{
    private final long id;

    private MutableIntObjectMap<Value> addedProperties;
    private MutableIntObjectMap<Value> changedProperties;
    private MutableIntSet removedProperties;

    PropertyContainerStateImpl( long id )
    {
        this.id = id;
    }

    public long getId()
    {
        return id;
    }

    void clear()
    {
        if ( changedProperties != null )
        {
            changedProperties.clear();
        }
        if ( addedProperties != null )
        {
            addedProperties.clear();
        }
        if ( removedProperties != null )
        {
            removedProperties.clear();
        }
    }

    void changeProperty( int propertyKeyId, Value value )
    {
        if ( addedProperties != null && addedProperties.containsKey( propertyKeyId ) )
        {
            addedProperties.put( propertyKeyId, value );
            return;
        }

        if ( changedProperties == null )
        {
            changedProperties = new IntObjectHashMap<>();
        }
        changedProperties.put( propertyKeyId, value );

        if ( removedProperties != null )
        {
            removedProperties.remove( propertyKeyId );
        }
    }

    void addProperty( int propertyKeyId, Value value )
    {
        if ( removedProperties != null && removedProperties.remove( propertyKeyId ) )
        {
            // This indicates the user did remove+add as two discrete steps, which should be translated to
            // a single change operation.
            changeProperty( propertyKeyId, value );
            return;
        }
        if ( addedProperties == null )
        {
            addedProperties = new IntObjectHashMap<>();
        }
        addedProperties.put( propertyKeyId, value );
    }

    void removeProperty( int propertyKeyId )
    {
        if ( addedProperties != null && addedProperties.remove( propertyKeyId ) != null )
        {
            return;
        }
        if ( removedProperties == null )
        {
            removedProperties = new IntHashSet();
        }
        removedProperties.add( propertyKeyId );
        if ( changedProperties != null )
        {
            changedProperties.remove( propertyKeyId );
        }
    }

    @Override
    public Iterator<StorageProperty> addedProperties()
    {
        return toPropertyIterator( addedProperties );
    }

    @Override
    public Iterator<StorageProperty> changedProperties()
    {
        return toPropertyIterator( changedProperties );
    }

    @Override
    public IntIterable removedProperties()
    {
        return removedProperties == null ? IntSets.immutable.empty() : removedProperties;
    }

    @Override
    public Iterator<StorageProperty> addedAndChangedProperties()
    {
        if ( addedProperties == null )
        {
            return toPropertyIterator( changedProperties );
        }
        if ( changedProperties == null )
        {
            return toPropertyIterator( addedProperties );
        }
        return Iterators.concat( toPropertyIterator( addedProperties ), toPropertyIterator( changedProperties ) );
    }

    @Override
    public boolean isPropertyChangedOrRemoved( int propertyKey )
    {
        return (removedProperties != null && removedProperties.contains( propertyKey ))
               || (changedProperties != null && changedProperties.containsKey( propertyKey ));
    }

    private Iterator<StorageProperty> toPropertyIterator( IntObjectMap<Value> propertyMap )
    {
        return propertyMap == null ? emptyIterator()
                                   : propertyMap.keyValuesView().collect( e -> (StorageProperty) new PropertyKeyValue( e.getOne(), e.getTwo() ) ).iterator();
    }
}
