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
package org.neo4j.kernel.impl.index.schema;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * Cache for lazily creating parts of the spatial index. Each part is created using the factory
 * the first time it is selected in a select() query.
 *
 * Iterating over the cache will return all currently created parts.
 *
 * @param <T> Type of parts
 * @param <E> Type of exception potentially thrown during creation
 */
class SpatialIndexCache<T, E extends Exception> implements Iterable<T>
{
    private final Factory<T, E> factory;

    private Map<CoordinateReferenceSystem,T> spatials = new CopyOnWriteHashMap<>();

    SpatialIndexCache( Factory<T, E> factory )
    {
        this.factory = factory;
    }

    /**
     * Select the part corresponding to the given CoordinateReferenceSystem. Creates the part if needed,
     * and rethrows any create time exception as a RuntimeException.
     *
     * @param crs target coordinate reference system
     * @return selected part
     */
    T uncheckedSelect( CoordinateReferenceSystem crs )
    {
        try
        {
            return select( crs );
        }
        catch ( Exception t )
        {
            throw new RuntimeException( t );
        }
    }

    /**
     * Select the part corresponding to the given CoordinateReferenceSystem. Creates the part if needed,
     * in which case an exception of type E might be thrown.
     *
     * @param crs target coordinate reference system
     * @return selected part
     * @throws E exception potentially thrown during creation
     */
    synchronized T select( CoordinateReferenceSystem crs ) throws E
    {
        T part = spatials.get( crs );
        if ( part == null )
        {
            part = factory.newSpatial( crs );
            if ( part != null )
            {
                spatials.put( crs, part );
            }
        }
        return part;
    }

    /**
     * Select the part corresponding to the given CoordinateReferenceSystem, apply function to it and return the result.
     * If the part isn't created yet return orElse.
     *
     * @param crs target coordinate reference system
     * @param function function to apply to part
     * @param orElse result to return if part isn't created yet
     * @param <RESULT> type of result
     * @return the result
     */
    <RESULT> RESULT selectOrElse( CoordinateReferenceSystem crs, Function<T, RESULT> function, RESULT orElse )
    {
        T part = spatials.get( crs );
        if ( part == null )
        {
            return orElse;
        }
        return function.apply( part );
    }

    void loadAll()
    {
        Iterator<CoordinateReferenceSystem> crsIterator = CoordinateReferenceSystem.all();
        while ( crsIterator.hasNext() )
        {
            uncheckedSelect( crsIterator.next() );
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return spatials.values().iterator();
    }

    /**
     * Factory used by the SpatialIndexCache to create parts.
     *
     * @param <T> Type of parts
     * @param <E> Type of exception potentially thrown during create
     */
    interface Factory<T, E extends Exception>
    {
        T newSpatial( CoordinateReferenceSystem crs ) throws E;
    }
}
