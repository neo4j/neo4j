/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.io.pagecache.tracing.cursor.CursorContext;

class BufferingIdGenerator extends IdGenerator.Delegate
{
    private DelayedBuffer<IdController.ConditionSnapshot> buffer;

    BufferingIdGenerator( IdGenerator delegate )
    {
        super( delegate );
    }

    void initialize( Supplier<IdController.ConditionSnapshot> boundaries, Predicate<IdController.ConditionSnapshot> safeThreshold )
    {
        buffer = new DelayedBuffer<>( boundaries, safeThreshold, 10_000, new FreeIdChunkConsumer() );
    }

    @Override
    public Marker marker( CursorContext cursorContext )
    {
        Marker actual = super.marker( cursorContext );
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
    public void maintenance( boolean awaitOngoing, CursorContext cursorContext )
    {
        // Check and potentially release ids onto the IdGenerator
        buffer.maintenance( cursorContext );

        // Do IdGenerator maintenance, typically ensure ID cache is full
        super.maintenance( awaitOngoing, cursorContext );
    }

    @Override
    public void clearCache( CursorContext cursorContext )
    {
        buffer.clear();

        super.clearCache( cursorContext );
    }

    void clear()
    {
        buffer.clear();
    }

    @Override
    public void checkpoint( CursorContext cursorContext )
    {
        // Flush buffered data to consumer
        buffer.maintenance( cursorContext );
        super.checkpoint( cursorContext );
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

    private class FreeIdChunkConsumer implements ChunkConsumer
    {
        @Override
        public void consume( long[] freedIds, CursorContext cursorContext )
        {
            try ( Marker reuseMarker = BufferingIdGenerator.super.marker( cursorContext ) )
            {
                for ( long id : freedIds )
                {
                    reuseMarker.markFree( id );
                }
            }
        }
    }
}
