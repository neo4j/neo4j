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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.IOException;
import org.neo4j.csv.reader.CharReadableChunker.ChunkImpl;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

/**
 * {@link InputChunk} parsing next entry on each call to {@link #next(InputEntityVisitor)}.
 */
public class CsvInputChunk implements InputChunk
{
    private final IdType idType;
    private final int delimiter;
    private final Collector badCollector;
    private final Chunk processingChunk;

    // Set in #initialize
    private CsvInputParser parser;
    private Decorator decorator;

    // Set as #next is called
    private InputEntityVisitor previousVisitor;
    private InputEntityVisitor visitor;
    private final Extractors extractors;

    public CsvInputChunk( IdType idType, int delimiter, Collector badCollector, Extractors extractors,
            ChunkImpl processingChunk )
    {
        this.idType = idType;
        this.badCollector = badCollector;
        this.extractors = extractors;
        this.delimiter = delimiter;
        this.processingChunk = processingChunk;
    }

    /**
     * Called every time this chunk is updated with new data. Potentially this data is from a different
     * stream of data than the previous, therefore the header and decorator is also updated.
     * @param seeker {@link CharSeeker} able to seek through the data.
     * @param header {@link Header} spec to read data according to.
     * @param decorator additional decoration of the {@link InputEntityVisitor} coming into
     * {@link #next(InputEntityVisitor)}.
     * @throws IOException on I/O error.
     */
    boolean initialize( CharSeeker seeker, Header header, Decorator decorator ) throws IOException
    {
        closeCurrentParser();
        this.decorator = decorator;
        this.visitor = null;
        this.parser = new CsvInputParser( seeker, delimiter, idType, header, badCollector, extractors );
        if ( header.entries().length == 0 )
        {
            return false;
        }
        return true;
    }

    private void closeCurrentParser() throws IOException
    {
        if ( parser != null )
        {
            parser.close();
        }
    }

    @Override
    public boolean next( InputEntityVisitor nakedVisitor ) throws IOException
    {
        if ( visitor == null || nakedVisitor != previousVisitor )
        {
            decorateVisitor( nakedVisitor );
        }

        return parser.next( visitor );
    }

    private void decorateVisitor( InputEntityVisitor nakedVisitor )
    {
        visitor = decorator.apply( nakedVisitor );
        previousVisitor = nakedVisitor;
    }

    protected Chunk processingChunk()
    {
        return processingChunk;
    }

    @Override
    public void close() throws IOException
    {
        closeCurrentParser();
    }
}
