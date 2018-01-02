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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.neo4j.unsafe.impl.batchimport.InputIterator;

/**
 * Takes an Iterator and chops it up into array batches downstream.
 */
public class IteratorBatcherStep<T> extends IoProducerStep
{
    private final InputIterator<T> data;
    private final Class<T> itemClass;

    public IteratorBatcherStep( StageControl control, Configuration config, InputIterator<T> data, Class<T> itemClass )
    {
        super( control, config );
        this.data = data;
        this.itemClass = itemClass;
    }

    @Override
    protected Object nextBatchOrNull( long ticket, int batchSize )
    {
        if ( !data.hasNext() )
        {
            return null;
        }

        @SuppressWarnings( "unchecked" )
        T[] batch = (T[]) Array.newInstance( itemClass, batchSize );
        int i = 0;
        for ( ; i < batchSize && data.hasNext(); i++ )
        {
            batch[i] = data.next();
        }
        return i == batchSize ? batch : Arrays.copyOf( batch, i );
    }

    @Override
    public void close()
    {
        data.close();
    }

    @Override
    protected long position()
    {
        return data.position();
    }
}
