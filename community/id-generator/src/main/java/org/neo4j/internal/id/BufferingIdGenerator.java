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
package org.neo4j.internal.id;

import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.io.pagecache.IOLimiter;

class BufferingIdGenerator extends IdGenerator.Delegate
{
    private DelayedBuffer<IdController.ConditionSnapshot> buffer;

    BufferingIdGenerator( IdGenerator delegate )
    {
        super( delegate );
    }

    void initialize( Supplier<IdController.ConditionSnapshot> boundaries, Predicate<IdController.ConditionSnapshot> safeThreshold )
    {
        buffer = new DelayedBuffer<>( boundaries, safeThreshold, 10_000, freedIds ->
        {
            try ( Marker reuseMarker = super.marker() )
            {
                for ( long id : freedIds )
                {
                    reuseMarker.markFree( id );
                }
            }
        } );
    }

    @Override
    public Marker marker()
    {
        Marker actual = super.marker();
        return new Marker()
        {
            @Override
            public void markUsed( long id )
            {
                // Goes straight in
                actual.markUsed( id );
            }

            @Override
            public void markDeleted( long id )
            {
                // Run these by the buffering too
                actual.markDeleted( id );
                buffer.offer( id );
            }

            @Override
            public void markFree( long id )
            {
                actual.markFree( id );
            }

            @Override
            public void close()
            {
                actual.close();
            }
        };
    }

    @Override
    public void maintenance()
    {
        // Check and potentially release ids onto the IdGenerator
        buffer.maintenance();

        // Do IdGenerator maintenance, typically ensure ID cache is full
        super.maintenance();
    }

    @Override
    public void clearCache()
    {
        buffer.clear();

        super.clearCache();
    }

    void clear()
    {
        buffer.clear();
    }

    @Override
    public void checkpoint( IOLimiter ioLimiter )
    {
        // Flush buffered data to consumer
        buffer.maintenance();
        super.checkpoint( ioLimiter );
    }

    @Override
    public void close()
    {
        if ( buffer != null )
        {
            buffer.close();
            buffer = null;
        }
        super.close();
    }
}
