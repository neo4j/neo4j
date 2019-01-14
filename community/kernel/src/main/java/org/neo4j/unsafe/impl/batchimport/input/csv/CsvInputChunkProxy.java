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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.csv.reader.Chunker;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

/**
 * {@link CsvInputChunk} that adapts new input source groups during the streaming of data.
 * {@link InputIterator} is fairly straight-forward, but is made a bit more complicated by the fact that
 * there can be multiple different data streams. The outer iterator, {@link CsvGroupInputIterator}, is still responsible
 * for handing out chunks, something that generally is good thing since it solves a bunch of other problems.
 * The problem it has is that it doesn't know exactly which type of {@link CsvInputChunk} it wants to create,
 * because that's up to each input group. This gap is bridged here in this class.
 */
public class CsvInputChunkProxy implements CsvInputChunk
{
    private CsvInputChunk actual;
    private int groupId = -1;

    public void ensureInstantiated( Supplier<CsvInputChunk> newChunk, int groupId ) throws IOException
    {
        if ( actual == null || groupId != this.groupId )
        {
            closeCurrent();
            actual = newChunk.get();
        }
        this.groupId = groupId;
    }

    @Override
    public void close() throws IOException
    {
        closeCurrent();
    }

    private void closeCurrent() throws IOException
    {
        if ( actual != null )
        {
            actual.close();
        }
    }

    @Override
    public boolean fillFrom( Chunker chunker ) throws IOException
    {
        return actual.fillFrom( chunker );
    }

    @Override
    public boolean next( InputEntityVisitor visitor ) throws IOException
    {
        return actual.next( visitor );
    }
}
