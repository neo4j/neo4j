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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * Cache for lazily creating parts of the spatial index. Each part is created using the factory
 * the first time it is selected in a select() query.
 *
 * Iterating over the cache will return all currently created parts.
 *
 * @param <T> Type of parts
 */
class SpatialIndexCache<T> extends IndexPartsCache<CoordinateReferenceSystem,T>
{
    private final Factory<T> factory;

    SpatialIndexCache( Factory<T> factory )
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
        T existing = cache.get( crs );
        if ( existing != null )
        {
            return existing;
        }

        // Instantiate from factory. Do this under lock so that we coordinate with any concurrent call to close.
        // Concurrent calls to instantiating parts won't contend with each other since there's only
        // a single writer at a time anyway.
        instantiateCloseLock.lock();
        try
        {
            assertOpen();
            return cache.computeIfAbsent( crs, key ->
            {
                try
                {
                    return factory.newSpatial( crs );
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            } );
        }
        finally
        {
            instantiateCloseLock.unlock();
        }
    }

    /**
     * Select the part corresponding to the given CoordinateReferenceSystem. Creates the part if needed,
     * in which case an exception of type E might be thrown.
     *
     * @param crs target coordinate reference system
     * @return selected part
     */
    T select( CoordinateReferenceSystem crs ) throws IOException
    {
        try
        {
            return uncheckedSelect( crs );
        }
        catch ( UncheckedIOException e )
        {
            throw e.getCause();
        }
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
        T part = cache.get( crs );
        if ( part == null )
        {
            return orElse;
        }
        return function.apply( part );
    }

    void loadAll()
    {
        for ( CoordinateReferenceSystem crs : CoordinateReferenceSystem.all() )
        {
            uncheckedSelect( crs );
        }
    }

    /**
     * Factory used by the SpatialIndexCache to create parts.
     *
     * @param <T> Type of parts
     */
    interface Factory<T>
    {
        T newSpatial( CoordinateReferenceSystem crs ) throws IOException;
    }
}
