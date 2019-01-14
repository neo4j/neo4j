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
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

import static org.neo4j.unsafe.impl.batchimport.input.csv.CsvInputIterator.seeker;

/**
 * {@link InputChunk} parsing next entry on each call to {@link #next(InputEntityVisitor)}.
 */
public class LazyCsvInputChunk implements CsvInputChunk
{
    private final IdType idType;
    private final int delimiter;
    private final Collector badCollector;
    private final Chunk processingChunk;
    private final Configuration config;
    private final Decorator decorator;
    private final Header header;
    private final Extractors extractors;

    // Set in #fillFrom
    private CsvInputParser parser;

    // Set as #next is called
    private InputEntityVisitor previousVisitor;
    private InputEntityVisitor visitor;

    public LazyCsvInputChunk( IdType idType, int delimiter, Collector badCollector, Extractors extractors, Chunk processingChunk, Configuration config,
            Decorator decorator, Header header )
    {
        this.idType = idType;
        this.badCollector = badCollector;
        this.extractors = extractors;
        this.delimiter = delimiter;
        this.processingChunk = processingChunk;
        this.config = config;
        this.decorator = decorator;
        this.header = header;
    }

    @Override
    public boolean fillFrom( Chunker chunker ) throws IOException
    {
        if ( chunker.nextChunk( processingChunk ) )
        {
            closeCurrentParser();
            this.visitor = null;
            this.parser = new CsvInputParser( seeker( processingChunk, config ), delimiter, idType, header.clone(), badCollector, extractors );
            return header.entries().length != 0;
        }
        return false;
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

    @Override
    public void close() throws IOException
    {
        closeCurrentParser();
    }
}
