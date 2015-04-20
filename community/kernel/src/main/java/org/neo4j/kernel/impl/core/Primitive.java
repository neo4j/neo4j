/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.store.CacheLoader;
import org.neo4j.kernel.impl.api.store.CacheUpdateListener;
import org.neo4j.kernel.impl.cache.SizeOfObject;
import org.neo4j.kernel.impl.store.InvalidRecordException;

public abstract class Primitive implements SizeOfObject
{
    // Used for marking that properties have been loaded but there just wasn't any.
    // Saves an extra trip down to the store layer.
    protected static final DefinedProperty[] NO_PROPERTIES = new DefinedProperty[0];

    public abstract long getId();

    public Iterator<DefinedProperty> getProperties( CacheLoader<Iterator<DefinedProperty>> loader,
                                                    CacheUpdateListener updateListener )
    {
        return ensurePropertiesLoaded( loader, updateListener );
    }

    public Property getProperty( CacheLoader<Iterator<DefinedProperty>> loader,
                                 CacheUpdateListener updateListener,
                                 int key )
    {
        ensurePropertiesLoaded( loader, updateListener );
        return getCachedProperty( key );
    }

    public PrimitiveLongIterator getPropertyKeys( CacheLoader<Iterator<DefinedProperty>> cacheLoader,
                                                  CacheUpdateListener updateListener )
    {
        ensurePropertiesLoaded( cacheLoader, updateListener );
        return getCachedPropertyKeys();
    }

    private Iterator<DefinedProperty> ensurePropertiesLoaded( CacheLoader<Iterator<DefinedProperty>> loader,
                                                              CacheUpdateListener updateListener )
    {
        if ( !hasLoadedProperties() )
        {
            synchronized ( this )
            {
                if ( !hasLoadedProperties() )
                {
                    try
                    {
                        Iterator<DefinedProperty> loadedProperties = loader.load( getId() );
                        setProperties( loadedProperties );
                        updateListener.newSize( this, sizeOfObjectInBytesIncludingOverhead() );
                    }
                    catch ( InvalidRecordException | EntityNotFoundException e )
                    {
                        throw new NotFoundException( this + " not found. This can be because someone " +
                                "else deleted this entity while we were trying to read properties from it, or because of " +
                                "concurrent modification of other properties on this entity. The problem should be temporary.", e );
                    }
                }
            }
        }
        return getCachedProperties();
    }

    protected abstract Iterator<DefinedProperty> getCachedProperties();

    protected abstract Property getCachedProperty( int key );

    protected abstract PrimitiveLongIterator getCachedPropertyKeys();

    protected abstract boolean hasLoadedProperties();

    protected abstract void setProperties( Iterator<DefinedProperty> properties );

    public abstract void commitPropertyMaps(
            PrimitiveIntObjectMap<DefinedProperty> cowPropertyAddMap, Iterator<Integer> removed );

    @Override
    public int hashCode()
    {
        long id = getId();
        return (int) (( id >>> 32 ) ^ id );
    }

    // Force subclasses to implement equals
    @Override
    public abstract boolean equals(Object other);
}
