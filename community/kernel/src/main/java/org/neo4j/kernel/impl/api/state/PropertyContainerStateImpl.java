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

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Predicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.versioned.VersionedPrimitiveLongObjectMap;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.values.storable.Value;

public class PropertyContainerStateImpl implements PropertyContainerState
{
    private final long id;

    private VersionedPrimitiveLongObjectMap<Value> addedProperties;
    private VersionedPrimitiveLongObjectMap<Value> changedProperties;
    private VersionedPrimitiveLongObjectMap<Value> removedProperties;

    private final Predicate<StorageProperty> excludePropertiesWeKnowAbout = new Predicate<StorageProperty>()
    {
        @Override
        public boolean test( StorageProperty item )
        {
            return (removedProperties == null || !removedProperties.currentView().containsKey( item.propertyKeyId() ))
                    && (addedProperties == null || !addedProperties.currentView().containsKey( item.propertyKeyId() ))
                    && (changedProperties == null || !changedProperties.currentView().containsKey( item.propertyKeyId() ));
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

    public void clear()
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
        if ( addedProperties != null )
        {
            if ( addedProperties.currentView().containsKey( propertyKeyId ) )
            {
                addedProperties.currentView().put( propertyKeyId, value );
                return;
            }
        }

        if ( changedProperties == null )
        {
            changedProperties = new VersionedPrimitiveLongObjectMap<>();
        }
        changedProperties.currentView().put( propertyKeyId, value );

        if ( removedProperties != null )
        {
            removedProperties.currentView().remove( propertyKeyId );
        }
    }

    void addProperty( int propertyKeyId, Value value )
    {
        if ( removedProperties != null )
        {
            Value removed = removedProperties.currentView().remove( propertyKeyId );
            if ( removed != null )
            {
                // This indicates the user did remove+add as two discrete steps, which should be translated to
                // a single change operation.
                changeProperty( propertyKeyId, value );
                return;
            }
        }
        if ( addedProperties == null )
        {
            addedProperties = new VersionedPrimitiveLongObjectMap<>();
        }
        addedProperties.currentView().put( propertyKeyId, value );
    }

    public void removeProperty( int propertyKeyId, Value value )
    {
        if ( addedProperties != null )
        {
            if ( addedProperties.currentView().remove( propertyKeyId ) != null )
            {
                return;
            }
        }
        if ( removedProperties == null )
        {
            removedProperties = new VersionedPrimitiveLongObjectMap<>();
        }
        removedProperties.currentView().put( propertyKeyId, value );
        if ( changedProperties != null )
        {
            changedProperties.currentView().remove( propertyKeyId );
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
    public PrimitiveLongIterator removedProperties()
    {
        return removedProperties != null ? removedProperties.currentView().iterator() : PrimitiveLongCollections.emptyIterator();
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

            if ( addedProperties != null && !addedProperties.currentView().isEmpty() )
            {
                iterator = Iterators.concat( iterator, toPropertyIterator( addedProperties ) );
            }
            if ( changedProperties != null && !changedProperties.currentView().isEmpty() )
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
        return (removedProperties != null && removedProperties.currentView().containsKey( propertyKey ))
                || (changedProperties != null && changedProperties.currentView().containsKey( propertyKey ));
    }

    @Override
    public boolean isPropertyRemoved( int propertyKeyId )
    {
        return removedProperties != null && removedProperties.currentView().containsKey( propertyKeyId );
    }

    private Iterator<StorageProperty> toPropertyIterator( VersionedPrimitiveLongObjectMap<Value> propertyMap )
    {
        return propertyMap == null ? Collections.emptyIterator() :
                Iterators.map(
                        entry -> new PropertyKeyValue( Math.toIntExact( entry ), propertyMap.currentView().get( entry ) ),
                        PrimitiveLongCollections.toIterator( propertyMap.currentView().iterator() )
                );
    }

    private PropertyKeyValue getPropertyOrNull( VersionedPrimitiveLongObjectMap<Value> propertyMap, int propertyKeyId )
    {
        Value value = propertyMap.currentView().get( propertyKeyId );
        return value == null ? null : new PropertyKeyValue( propertyKeyId, value );
    }
}
