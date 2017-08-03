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

import java.util.function.LongConsumer;
import java.util.function.Supplier;

import org.neo4j.graphdb.Resource;

import static org.neo4j.kernel.impl.store.id.IdRangeIterator.VALUE_REPRESENTING_NULL;

public class RenewableBatchIdSequence implements IdSequence, Resource
{
    private final Supplier<IdRangeIterator> source;
    private IdRangeIterator currentBatch;
    private final LongConsumer excessIdConsumer;

    public RenewableBatchIdSequence( Supplier<IdRangeIterator> source, LongConsumer excessIdConsumer )
    {
        this.source = source;
        this.excessIdConsumer = excessIdConsumer;
    }

    @Override
    public void close()
    {
        if ( currentBatch != null )
        {
            long id;
            while ( (id = currentBatch.next()) != VALUE_REPRESENTING_NULL )
            {
                excessIdConsumer.accept( id );
            }
        }
    }

    @Override
    public long nextId()
    {
        long id;
        if ( currentBatch == null || (id = currentBatch.next()) == VALUE_REPRESENTING_NULL )
        {
            currentBatch = source.get();
            id = currentBatch.next();
        }
        return id;
    }

    @Override
    public IdRange nextIdBatch( int size )
    {
        throw new UnsupportedOperationException( "Haven't been needed so far" );
    }
}
