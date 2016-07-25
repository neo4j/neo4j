/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Iterator;
import java.util.function.Predicate;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.CombiningIterator;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.util.VersionedHashMap;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;

import static org.neo4j.helpers.collection.Iterators.emptyIterator;

public class PropertyContainerStateImpl implements PropertyContainerState
{
    private final long id;
    private static final ResourceIterator<StorageProperty> NO_PROPERTIES = emptyIterator();

    private VersionedHashMap<Integer, StorageProperty> addedProperties;
    private VersionedHashMap<Integer, StorageProperty> changedProperties;
    private VersionedHashMap<Integer, StorageProperty> removedProperties;

    private final Predicate<StorageProperty> excludePropertiesWeKnowAbout = new Predicate<StorageProperty>()
    {
        @Override
        public boolean test( StorageProperty item )
        {
            return (removedProperties == null || !removedProperties.containsKey( item.propertyKeyId() ))
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

    public void changeProperty( DefinedProperty property )
    {
        if ( addedProperties != null )
        {
            if ( addedProperties.containsKey( property.propertyKeyId() ) )
            {
                addedProperties.put( property.propertyKeyId(), property );
                return;
            }
        }

        if ( changedProperties == null )
        {
            changedProperties = new VersionedHashMap<>();
        }
        changedProperties.put( property.propertyKeyId(), property );
        if ( removedProperties != null )
        {
            removedProperties.remove( property.propertyKeyId() );
        }
    }

    public void addProperty( DefinedProperty property )
    {
        if ( removedProperties != null )
        {
            StorageProperty removed = removedProperties.remove( property.propertyKeyId() );
            if ( removed != null )
            {
                // This indicates the user did remove+add as two discrete steps, which should be translated to
                // a single change operation.
                changeProperty( property );
                return;
            }
        }
        if ( addedProperties == null )
        {
            addedProperties = new VersionedHashMap<>();
        }
        addedProperties.put( property.propertyKeyId(), property );
    }

    public void removeProperty( DefinedProperty property )
    {
        if ( addedProperties != null )
        {
            if ( addedProperties.remove( property.propertyKeyId() ) != null )
            {
                return;
            }
        }
        if ( removedProperties == null )
        {
            removedProperties = new VersionedHashMap<>();
        }
        removedProperties.put( property.propertyKeyId(), property );
        if ( changedProperties != null )
        {
            changedProperties.remove( property.propertyKeyId() );
        }
    }

    @Override
    public Iterator<StorageProperty> addedProperties()
    {
        return addedProperties != null ? addedProperties.values().iterator() : NO_PROPERTIES;
    }

    @Override
    public Iterator<StorageProperty> changedProperties()
    {
        return changedProperties != null ? changedProperties.values().iterator() : NO_PROPERTIES;
    }

    @Override
    public Iterator<Integer> removedProperties()
    {
        return removedProperties != null ? removedProperties.keySet().iterator()
                : Iterators.<Integer>emptyIterator();
    }

    @Override
    public Iterator<StorageProperty> addedAndChangedProperties()
    {
        Iterator<StorageProperty> out = null;
        if ( addedProperties != null )
        {
            out = addedProperties.values().iterator();
        }
        if ( changedProperties != null )
        {
            if ( out != null )
            {
                out = new CombiningIterator<>(
                        Iterators.iterator( out, changedProperties.values().iterator() ) );
            }
            else
            {
                out = changedProperties.values().iterator();
            }
        }
        return out != null ? out : NO_PROPERTIES;
    }

    @Override
    public Iterator<StorageProperty> augmentProperties( Iterator<StorageProperty> iterator )
    {
        if ( removedProperties != null || addedProperties != null || changedProperties != null )
        {
            iterator = new FilteringIterator<>( iterator, excludePropertiesWeKnowAbout );

            if ( addedProperties != null && !addedProperties.isEmpty() )
            {
                iterator = new CombiningIterator<>(
                        Iterators.iterator( iterator, addedProperties.values().iterator() ) );
            }
            if ( changedProperties != null && !changedProperties.isEmpty() )
            {
                iterator = new CombiningIterator<>(
                        Iterators.iterator( iterator, changedProperties.values().iterator() ) );
            }
        }

        return iterator;
    }

    @Override
    public void accept( Visitor visitor ) throws ConstraintValidationKernelException
    {
        if ( addedProperties != null || removedProperties != null || changedProperties != null )
        {
            visitor.visitPropertyChanges( id, addedProperties(), changedProperties(), removedProperties() );
        }
    }

    @Override
    public boolean hasChanges()
    {
        return addedProperties != null || removedProperties != null || changedProperties != null;
    }

    @Override
    public StorageProperty getChangedProperty( int propertyKeyId )
    {
        return changedProperties != null ? changedProperties.get( propertyKeyId ) : null;
    }

    @Override
    public StorageProperty getAddedProperty( int propertyKeyId )
    {
        return addedProperties != null ? addedProperties.get( propertyKeyId ) : null;
    }

    @Override
    public boolean isPropertyRemoved( int propertyKeyId )
    {
        return removedProperties != null && removedProperties.containsKey( propertyKeyId );
    }
}
