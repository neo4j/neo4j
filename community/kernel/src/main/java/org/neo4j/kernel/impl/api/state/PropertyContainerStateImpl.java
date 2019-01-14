/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.kernel.impl.util.VersionedHashMap;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.newSetFromMap;

class PropertyContainerStateImpl implements PropertyContainerState
{
    private final long id;

    private VersionedHashMap<Integer, Value> addedProperties;
    private VersionedHashMap<Integer, Value> changedProperties;
    private Set<Integer> removedProperties;

    private final Predicate<StorageProperty> excludePropertiesWeKnowAbout = new Predicate<StorageProperty>()
    {
        @Override
        public boolean test( StorageProperty item )
        {
            return (removedProperties == null || !removedProperties.contains( item.propertyKeyId() ))
                    && (addedProperties == null || !addedProperties.containsKey( item.propertyKeyId() ))
                    && (changedProperties == null || !changedProperties.containsKey( item.propertyKeyId() ));
        }
    };

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
            changedProperties = new VersionedHashMap<>();
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
            addedProperties = new VersionedHashMap<>();
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
            removedProperties = newSetFromMap( new VersionedHashMap<>() );
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
    public Iterator<Integer> removedProperties()
    {
        return removedProperties != null ? removedProperties.iterator() : emptyIterator();
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
    public Iterator<StorageProperty> augmentProperties( Iterator<StorageProperty> iterator )
    {
        if ( removedProperties != null || addedProperties != null || changedProperties != null )
        {
            iterator = Iterators.filter( excludePropertiesWeKnowAbout, iterator );

            if ( addedProperties != null && !addedProperties.isEmpty() )
            {
                iterator = Iterators.concat( iterator, toPropertyIterator( addedProperties ) );
            }
            if ( changedProperties != null && !changedProperties.isEmpty() )
            {
                iterator = Iterators.concat( iterator, toPropertyIterator( changedProperties ) );
            }
        }

        return iterator;
    }

    @Override
    public void accept( Visitor visitor ) throws ConstraintValidationException
    {
        if ( addedProperties != null || removedProperties != null || changedProperties != null )
        {
            visitor.visitPropertyChanges( id, addedProperties(), changedProperties(), removedProperties() );
        }
    }

    @Override
    public boolean hasPropertyChanges()
    {
        return addedProperties != null || removedProperties != null || changedProperties != null;
    }

    @Override
    public StorageProperty getChangedProperty( int propertyKeyId )
    {
        return changedProperties == null ? null : getPropertyOrNull( changedProperties, propertyKeyId );
    }

    @Override
    public StorageProperty getAddedProperty( int propertyKeyId )
    {
        return addedProperties == null ? null : getPropertyOrNull( addedProperties, propertyKeyId );
    }

    @Override
    public boolean isPropertyChangedOrRemoved( int propertyKey )
    {
        return (removedProperties != null && removedProperties.contains( propertyKey ))
               || (changedProperties != null && changedProperties.containsKey( propertyKey ));
    }

    @Override
    public boolean isPropertyRemoved( int propertyKeyId )
    {
        return removedProperties != null && removedProperties.contains( propertyKeyId );
    }

    private Iterator<StorageProperty> toPropertyIterator( VersionedHashMap<Integer,Value> propertyMap )
    {
        return propertyMap == null ? emptyIterator() :
               Iterators.map(
                    entry -> new PropertyKeyValue( entry.getKey(), entry.getValue() ),
                    propertyMap.entrySet().iterator()
                );
    }

    private PropertyKeyValue getPropertyOrNull( VersionedHashMap<Integer,Value> propertyMap, int propertyKeyId )
    {
        Value value = propertyMap.get( propertyKeyId );
        return value == null ? null : new PropertyKeyValue( propertyKeyId, value );
    }
}
