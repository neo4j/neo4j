/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.Iterator;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.CacheLoader;
import org.neo4j.kernel.impl.api.CacheUpdateListener;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.cache.SizeOfObject;
import org.neo4j.kernel.impl.core.WritableTransactionState.CowEntityElement;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;

import static org.neo4j.helpers.collection.Iterables.map;

public abstract class Primitive implements SizeOfObject
{
    // Used for marking that properties have been loaded but there just wasn't any.
    // Saves an extra trip down to the store layer.
    protected static final Property[] NO_PROPERTIES = new Property[0];

    Primitive( boolean newPrimitive )
    {
        if ( newPrimitive ) setEmptyProperties();
    }

    public abstract long getId();
    
    public Iterator<Property> getProperties( StatementState state, CacheLoader<Iterator<Property>> loader,
            CacheUpdateListener updateListener )
    {
        return ensurePropertiesLoaded( state, loader, updateListener );
    }

    public Property getProperty( StatementState state, CacheLoader<Iterator<Property>> loader,
            CacheUpdateListener updateListener, int key )
    {
        ensurePropertiesLoaded( state, loader, updateListener );
        return getCachedProperty( key );
    }
    
    public PrimitiveLongIterator getPropertyKeys( StatementState state, CacheLoader<Iterator<Property>> cacheLoader,
            CacheUpdateListener updateListener )
    {
        ensurePropertiesLoaded( state, cacheLoader, updateListener );
        return getCachedPropertyKeys();
    }
    
    private Iterator<Property> ensurePropertiesLoaded( StatementState state, CacheLoader<Iterator<Property>> loader,
            CacheUpdateListener updateListener )
    {
        if ( !hasLoadedProperties() ) synchronized ( this )
        {
            if ( !hasLoadedProperties() )
            {
                try
                {
                    Iterator<Property> loadedProperties = loader.load( state, getId() );
                    setProperties( loadedProperties );
                    updateListener.newSize( this, sizeOfObjectInBytesIncludingOverhead() );
                }
                catch ( InvalidRecordException e )
                {
                    throw new NotFoundException( this + " not found. This can be because someone " +
                            "else deleted this entity while we were trying to read properties from it, or because of " +
                            "concurrent modification of other properties on this entity. The problem should be temporary.", e );
                }
                catch ( EntityNotFoundException e )
                {
                    throw new NotFoundException( this + " not found. This can be because someone " +
                            "else deleted this entity while we were trying to read properties from it, or because of " +
                            "concurrent modification of other properties on this entity. The problem should be temporary.", e );
                }
            }
        }
        return getCachedProperties();
    }

    protected abstract Iterator<Property> getCachedProperties();
    
    protected abstract Property getCachedProperty( int key );
    
    protected abstract PrimitiveLongIterator getCachedPropertyKeys();

    protected abstract boolean hasLoadedProperties();

    protected abstract void setEmptyProperties();
    
    protected abstract void setProperties( Iterator<Property> properties );
    
    protected abstract PropertyData getPropertyForIndex( int keyId );
    
    protected abstract void commitPropertyMaps(
            ArrayMap<Integer, PropertyData> cowPropertyAddMap,
            ArrayMap<Integer, PropertyData> cowPropertyRemoveMap, long firstProp );

    @Override
    public int hashCode()
    {
        long id = getId();
        return (int) (( id >>> 32 ) ^ id );
    }

    // Force subclasses to implement equals
    @Override
    public abstract boolean equals(Object other);

    private Object getPropertyValue( NodeManager nodeManager, PropertyData property )
    {
        Object value = property.getValue();
        if ( value == null )
        {
            /*
             * This will only happen for "heavy" property value, such as
             * strings/arrays
             */
            value = loadPropertyValue( nodeManager, property.getIndex() );
            property.setNewValue( value );
        }
        return value;
    }

    private void ensurePropertiesLoaded( NodeManager nodeManager )
    {
        // double checked locking
        if ( !hasLoadedProperties() ) synchronized ( this )
        {
            if ( !hasLoadedProperties() )
            {
                try
                {
                    ArrayMap<Integer, PropertyData> loadedProperties = loadProperties( nodeManager );
                    setProperties( toPropertyIterator( loadedProperties ) );
                }
                catch ( InvalidRecordException e )
                {
                    throw new NotFoundException( asProxy( nodeManager ) + " not found. This can be because someone " +
                            "else deleted this entity while we were trying to read properties from it, or because of " +
                            "concurrent modification of other properties on this entity. The problem should be temporary.", e );
                }
            }
        }
    }

    private Iterator<Property> toPropertyIterator( ArrayMap<Integer, PropertyData> loadedProperties )
    {
        return map( new Function<PropertyData, Property>()
        {
            @Override
            public Property apply( PropertyData from )
            {
                return Property.property( from.getIndex(), from.getValue() );
            }
        }, loadedProperties.values() ).iterator();
    }

    protected abstract ArrayMap<Integer, PropertyData> loadProperties( NodeManager nodeManager );
    
    protected abstract Object loadPropertyValue( NodeManager nodeManager, int propertyKey );

    protected Object getCommittedPropertyValue( NodeManager nodeManager, String key )
    {
        ensurePropertiesLoaded( nodeManager );
        Token index = nodeManager.getPropertyKeyTokenOrNull( key );
        if ( index != null )
        {
            PropertyData property = getPropertyForIndex( index.id() );
            if ( property != null )
            {
                return getPropertyValue( nodeManager, property );
            }
        }
        return null;
    }
    
    public abstract CowEntityElement getEntityElement( PrimitiveElement element, boolean create );
    
    abstract PropertyContainer asProxy( NodeManager nm );
}