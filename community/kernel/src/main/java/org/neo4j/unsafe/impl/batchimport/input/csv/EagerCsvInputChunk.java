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

import org.neo4j.csv.reader.Chunker;
import org.neo4j.csv.reader.Source;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

class EagerCsvInputChunk implements CsvInputChunk, Source.Chunk
{
    private InputEntity[] entities;
    private int cursor;

    void initialize( InputEntity[] entities )
    {
        this.entities = entities;
        this.cursor = 0;
    }

    @Override
    public boolean next( InputEntityVisitor visitor ) throws IOException
    {
        if ( cursor < entities.length )
        {
            entities[cursor++].replayOnto( visitor );
            return true;
        }
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean fillFrom( Chunker chunker ) throws IOException
    {
        return chunker.nextChunk( this );
    }

    @Override
    public char[] data()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int length()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int maxFieldSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String sourceDescription()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int startPosition()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int backPosition()
    {
        throw new UnsupportedOperationException();
    }
}
