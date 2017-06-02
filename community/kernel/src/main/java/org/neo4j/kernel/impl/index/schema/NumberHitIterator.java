/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;

/**
 * Wraps number key/value results in a {@link PrimitiveLongIterator}.
 * The {@link RawCursor seeker} which gets passed in will have to be closed somewhere else because
 * the {@link PrimitiveLongIterator} is just a plain iterator, no resource.
 *
 * @param <KEY> type of {@link NumberKey}.
 * @param <VALUE> type of {@link NumberValue}.
 */
public class NumberHitIterator<KEY extends NumberKey, VALUE extends NumberValue>
        extends PrimitiveLongCollections.PrimitiveLongBaseIterator
{
    private final RawCursor<Hit<KEY,VALUE>,IOException> seeker;

    NumberHitIterator( RawCursor<Hit<KEY,VALUE>,IOException> seeker )
    {
        this.seeker = seeker;
    }

    @Override
    protected boolean fetchNext()
    {
        try
        {
            return seeker.next() && next( seeker.get().key().entityId );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
