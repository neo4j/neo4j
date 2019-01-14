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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

class CachingInputIterable implements InputIterable
{
    private final InputIterable actual;
    private final InputCache cache;
    private boolean firstTime = true;

    CachingInputIterable( InputIterable actual, InputCache cache )
    {
        this.cache = cache;
        this.actual = actual;
    }

    @Override
    public boolean supportsMultiplePasses()
    {
        return true;
    }

    @Override
    public InputIterator iterator()
    {
        if ( actual.supportsMultiplePasses() )
        {
            // best-case, we don't need to wrap this since it already supports multiple passes
            return actual.iterator();
        }

        try
        {
            if ( firstTime )
            {
                // wrap in an iterator which caches the data as it goes over it
                firstTime = false;
                InputCacher cacher = cache.cacheNodes();
                return new CachingInputIterator( actual.iterator(), cacher );
            }
            // for consecutive iterations just returned the cached data
            return cache.nodes().iterator();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
