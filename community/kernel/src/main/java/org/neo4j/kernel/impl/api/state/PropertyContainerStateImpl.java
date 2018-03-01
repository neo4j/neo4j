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
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.versioned.VersionedPrimitiveLongObjectMap;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.values.storable.Value;

import static org.neo4j.collection.primitive.base.Empty.EMPTY_PRIMITIVE_LONG_OBJECT_MAP;

public class PropertyContainerStateImpl implements PropertyContainerState
{
    private final long id;
    final StateSelector stateSelector;
    VersionedPrimitiveLongObjectMap<Value> addedProperties;
    VersionedPrimitiveLongObjectMap<Value> changedProperties;
    VersionedPrimitiveLongObjectMap<Value> removedProperties;

    private final Predicate<StorageProperty> excludePropertiesWeKnowAbout = new Predicate<StorageProperty>()
    {
        @Override
        public boolean test( StorageProperty item )
        {
            return (removedProperties == null || !removedPropertiesView().containsKey( item.propertyKeyId() ))
                    && (addedProperties == null || !addedPropertiesView().containsKey( item.propertyKeyId() ))
                    && (changedProperties == null || !changedPropertiesView().containsKey( item.propertyKeyId() ));
        }
    };

    PropertyContainerStateImpl( long id )
    {
        this( id, StateSelector.CURRENT_STATE );
    }

    PropertyContainerStateImpl( long id, StateSelector stateSelector )
    {
        this.id = id;
        this.stateSelector = stateSelector;
    }

    PropertyContainerStateImpl( PropertyContainerStateImpl containerState, StateSelector stateSelector )
    {
        this( containerState.id, stateSelector );
        this.addedProperties  = containerState.addedProperties;
        this.changedProperties  = containerState.changedProperties;
        this.removedProperties  = containerState.removedProperties;
    }

    PrimitiveLongObjectMap<Value> changedPropertiesView()
    {
        return changedProperties != null ? stateSelector.getView( changedProperties ) : EMPTY_PRIMITIVE_LONG_OBJECT_MAP;
    }

    PrimitiveLongObjectMap<Value> addedPropertiesView()
    {
        return addedProperties != null ?  stateSelector.getView( addedProperties ) : EMPTY_PRIMITIVE_LONG_OBJECT_MAP;
    }

    PrimitiveLongObjectMap<Value> removedPropertiesView()
    {
        return removedProperties != null ? stateSelector.getView( removedProperties ) : EMPTY_PRIMITIVE_LONG_OBJECT_MAP;
    }

    void markStable()
    {
        if ( addedProperties != null )
        {
            addedProperties.markStable();
        }
        if ( changedProperties != null )
        {
            changedProperties.markStable();
        }
        if ( removedProperties != null )
        {
            removedProperties.markStable();
        }
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
            PrimitiveLongObjectMap<Value> addedPropertiesView = addedProperties.currentView();
            if ( addedPropertiesView.containsKey( propertyKeyId ) )
            {
                addedPropertiesView.put( propertyKeyId, value );
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
        return toPropertyIterator( addedPropertiesView() );
    }

    @Override
    public Iterator<StorageProperty> changedProperties()
    {
        return toPropertyIterator( changedPropertiesView() );
    }

    @Override
    public PrimitiveLongIterator removedProperties()
    {
        return removedPropertiesView().iterator();
    }

    @Override
    public Iterator<StorageProperty> addedAndChangedProperties()
    {
        if ( addedProperties == null )
        {
            return toPropertyIterator( changedPropertiesView() );
        }
        if ( changedProperties == null )
        {
            return toPropertyIterator( addedPropertiesView() );
        }
        return Iterators.concat( toPropertyIterator( addedPropertiesView() ), toPropertyIterator( changedPropertiesView() ) );
    }

    @Override
    public Iterator<StorageProperty> augmentProperties( Iterator<StorageProperty> iterator )
    {
        if ( removedProperties != null || addedProperties != null || changedProperties != null )
        {
            iterator = Iterators.filter( excludePropertiesWeKnowAbout, iterator );

            if ( addedProperties != null && !addedPropertiesView().isEmpty() )
            {
                iterator = Iterators.concat( iterator, toPropertyIterator( addedPropertiesView() ) );
            }
            if ( changedProperties != null && !changedPropertiesView().isEmpty() )
            {
                iterator = Iterators.concat( iterator, toPropertyIterator( changedPropertiesView() ) );
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
        return changedProperties == null ? null : getPropertyOrNull( propertyKeyId, changedProperties.currentView() );
    }

    @Override
    public StorageProperty getAddedProperty( int propertyKeyId )
    {
        return addedProperties == null ? null : getPropertyOrNull( propertyKeyId, addedProperties.currentView() );
    }

    @Override
    public boolean isPropertyChangedOrRemoved( int propertyKey )
    {
        return isPropertyRemoved( propertyKey ) || (changedProperties != null && changedPropertiesView().containsKey( propertyKey ));
    }

    @Override
    public boolean isPropertyRemoved( int propertyKeyId )
    {
        return removedProperties != null && removedPropertiesView().containsKey( propertyKeyId );
    }

    Iterator<StorageProperty> toPropertyIterator( PrimitiveLongObjectMap<Value> valuePrimitiveLongObjectMap )
    {
        return valuePrimitiveLongObjectMap.isEmpty() ? Collections.emptyIterator() :
                Iterators.map(
                        entry -> new PropertyKeyValue( Math.toIntExact( entry ), valuePrimitiveLongObjectMap.get( entry ) ),
                        PrimitiveLongCollections.toIterator( valuePrimitiveLongObjectMap.iterator() )
                );
    }

    private PropertyKeyValue getPropertyOrNull( int propertyKeyId, PrimitiveLongObjectMap<Value> valuePrimitiveLongObjectMap )
    {
        Value value = valuePrimitiveLongObjectMap.get( propertyKeyId );
        return value == null ? null : new PropertyKeyValue( propertyKeyId, value );
    }
}
