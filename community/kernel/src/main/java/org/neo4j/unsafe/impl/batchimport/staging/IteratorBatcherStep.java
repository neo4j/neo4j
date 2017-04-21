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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Predicate;

import org.neo4j.unsafe.impl.batchimport.Configuration;

/**
 * Takes an Iterator and chops it up into array batches downstream.
 */
public abstract class IteratorBatcherStep<T> extends PullingProducerStep
{
    private final Iterator<T> data;
    private final Class<T> itemClass;
    private final Predicate<T> filter;

    protected long cursor;
    private T[] batch;
    private int batchCursor;
    private int skipped;

    public IteratorBatcherStep( StageControl control, Configuration config, Iterator<T> data, Class<T> itemClass,
            Predicate<T> filter )
    {
        super( control, config );
        this.data = data;
        this.itemClass = itemClass;
        this.filter = filter;
        newBatch();
    }

    @SuppressWarnings( "unchecked" )
    private void newBatch()
    {
        batchCursor = 0;
        batch = (T[]) Array.newInstance( itemClass, batchSize );
    }

    @Override
    protected Object nextBatchOrNull( long ticket, int batchSize )
    {
        while ( data.hasNext() )
        {
            T candidate = data.next();
            if ( filter.test( candidate ) )
            {
                batch[batchCursor++] = candidate;
                cursor++;
                if ( batchCursor == batchSize )
                {
                    T[] result = batch;
                    newBatch();
                    return result;
                }
            }
            else
            {
                if ( ++skipped == batchSize )
                {
                    skipped = 0;
                    return Array.newInstance( itemClass, 0 );
                }
            }
        }

        if ( batchCursor == 0 )
        {
            return null; // marks the end
        }
        try
        {
            return batchCursor == batchSize ? batch : Arrays.copyOf( batch, batchCursor );
        }
        finally
        {
            batchCursor = 0;
        }
    }
}
