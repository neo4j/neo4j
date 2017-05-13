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
package org.neo4j.tooling;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.kernel.impl.util.collection.ContinuableArrayCursor;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.IdRangeInput.Range;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.staging.TicketedProcessing;

import static org.neo4j.unsafe.impl.batchimport.IdRangeInput.idRangeInput;

/**
 * Data generator as {@link InputIterator}, parallelizable
 */
public class EntityDataGenerator<T> extends InputIterator.Adapter<T>
{
    private final String sourceDescription;
    private final TicketedProcessing<Range,Void,T[]> processing;

    private long cursor;
    private final ContinuableArrayCursor<T> itemCursor;

    public EntityDataGenerator( Function<Range,T[]> generator, long count )
    {
        this.sourceDescription = getClass().getSimpleName();
        BiFunction<Range,Void,T[]> processor = ( batch, ignore ) -> generator.apply( batch );
        this.processing = new TicketedProcessing<>( getClass().getName(),
                Runtime.getRuntime().availableProcessors(), processor, () -> null );
        this.processing.slurp( idRangeInput( count, Configuration.DEFAULT.batchSize() ), true );
        this.itemCursor = new ContinuableArrayCursor<>( processing::next );
    }

    @Override
    protected T fetchNextOrNull()
    {
        if ( itemCursor.next() )
        {
            cursor++;
            return itemCursor.get();
        }
        return null;
    }

    @Override
    public String sourceDescription()
    {
        return sourceDescription;
    }

    @Override
    public long lineNumber()
    {
        return cursor;
    }

    @Override
    public long position()
    {
        return 0;
    }

    @Override
    public void close()
    {
        super.close();
        processing.close();
    }

    @Override
    public int processors( int delta )
    {
        return processing.processors( delta );
    }
}
