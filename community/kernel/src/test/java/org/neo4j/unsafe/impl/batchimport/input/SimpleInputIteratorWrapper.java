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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

/**
 * Makes an {@link Iterator} provide {@link SourceTraceability source information}.
 */
public class SimpleInputIteratorWrapper extends SimpleInputIterator
{
    private final Iterator<InputChunk> source;

    public SimpleInputIteratorWrapper( String sourceDescription, int batchSize, Iterator<InputChunk> source )
    {
        super( sourceDescription, batchSize );
        this.source = source;
    }

    @Override
    protected InputChunk fetchNextOrNull()
    {
        return source.hasNext() ? source.next() : null;
    }

    public static InputIterable wrap( final String sourceDescription, final Iterable<InputChunk> source )
    {
        return new InputIterable()
        {
            @Override
            public InputIterator iterator()
            {
                return new SimpleInputIteratorWrapper<>( sourceDescription, source.iterator() );
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    @Override
    public InputChunk newChunk()
    {
        return null;
    }

    @Override
    public boolean next( InputChunk chunk ) throws IOException
    {
        return source.hasNext() ? source.next() : null;
    }
}
