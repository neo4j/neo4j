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
package org.neo4j.kernel.impl.store.id;

import java.util.function.Supplier;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;

public class RenewableIdBatch
        extends PrimitiveLongCollections.PrimitiveLongBaseIterator
        implements PrimitiveLongResourceIterator
{
    private final Supplier<IdRangeIterator> source;
    private IdRangeIterator currentBatch;

    public RenewableIdBatch( Supplier<IdRangeIterator> source )
    {
        this.source = source;
    }

    @Override
    public void close()
    {
        // todo release excess ids back to id generator
    }

    @Override
    protected boolean fetchNext()
    {
        long id;
        if ( currentBatch == null || (id = currentBatch.next()) == IdRangeIterator.VALUE_REPRESENTING_NULL )
        {
            currentBatch = source.get();
            id = currentBatch.next();
        }
        return next( id );
    }
}
