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

import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Chunker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

import static org.neo4j.csv.reader.CharSeekers.charSeeker;

/**
 * {@link Chunker} which parses a chunk of entities when calling {@link #nextChunk(Chunk)},
 * injecting them into {@link EagerCsvInputChunk}, which simply hands them out one by one.
 */
public class EagerParserChunker implements Chunker
{
    private final CharSeeker seeker;
    private final CsvInputParser parser;
    private final int chunkSize;
    private final Decorator decorator;

    public EagerParserChunker( CharReadable reader, IdType idType, Header header, Collector badCollector, Extractors extractors,
            int chunkSize, Configuration config, Decorator decorator )
    {
        this.chunkSize = chunkSize;
        this.decorator = decorator;
        this.seeker = charSeeker( reader, config, true );
        this.parser = new CsvInputParser( seeker, config.delimiter(), idType, header, badCollector, extractors );
    }

    @Override
    public boolean nextChunk( Chunk chunk ) throws IOException
    {
        InputEntityArray entities = new InputEntityArray( chunkSize );
        InputEntityVisitor decorated = decorator.apply( entities );
        int cursor = 0;
        for ( ; cursor < chunkSize && parser.next( decorated ); cursor++ )
        {   // just loop through and parse
        }

        if ( cursor > 0 )
        {
            ((EagerCsvInputChunk)chunk).initialize( entities.toArray() );
            return true;
        }
        return false;
    }

    @Override
    public long position()
    {
        return seeker.position();
    }

    @Override
    public void close() throws IOException
    {
        parser.close();
    }

    @Override
    public Chunk newChunk()
    {
        throw new UnsupportedOperationException();
    }
}
