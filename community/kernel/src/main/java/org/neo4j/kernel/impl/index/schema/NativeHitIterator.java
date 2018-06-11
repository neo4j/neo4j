/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.api.iterator.LongIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.values.storable.Value;

/**
 * Wraps number key/value results in a {@link LongIterator}.
 *
 * @param <KEY> type of {@link NumberIndexKey}.
 * @param <VALUE> type of {@link NativeIndexValue}.
 */
public class NativeHitIterator<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        extends PrimitiveLongCollections.PrimitiveLongBaseIterator
        implements PrimitiveLongResourceIterator
{
    private final RawCursor<Hit<KEY,VALUE>,IOException> seeker;
    private final Collection<RawCursor<Hit<KEY,VALUE>,IOException>> toRemoveFromWhenExhausted;
    private boolean closed;

    NativeHitIterator( RawCursor<Hit<KEY,VALUE>,IOException> seeker,
            Collection<RawCursor<Hit<KEY,VALUE>,IOException>> toRemoveFromWhenExhausted )
    {
        this.seeker = seeker;
        this.toRemoveFromWhenExhausted = toRemoveFromWhenExhausted;
    }

    @Override
    protected boolean fetchNext()
    {
        try
        {
            while ( seeker.next() )
            {
                KEY key = seeker.get().key();
                if ( acceptValue( key.asValue() ) )
                {
                    return next( key.getEntityId() );
                }
            }
            return false;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    boolean acceptValue( Value value )
    {
        return true;
    }

    private void ensureCursorClosed() throws IOException
    {
        if ( !closed )
        {
            seeker.close();
            toRemoveFromWhenExhausted.remove( seeker );
            closed = true;
        }
    }

    @Override
    public void close()
    {
        try
        {
            ensureCursorClosed();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
