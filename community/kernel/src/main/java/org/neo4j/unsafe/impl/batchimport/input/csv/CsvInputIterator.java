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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

import org.neo4j.csv.reader.BufferedCharSeeker;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharReadableChunker;
import org.neo4j.csv.reader.CharReadableChunker.ProcessingChunk;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Source;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;

public class CsvInputIterator extends InputIterator.Adapter
{
    private final Iterator<DataFactory> source;
    private final Header.Factory headerFactory;
    private final IdType idType;
    private final Configuration config;
    private final Collector badCollector;
    private Single current;

    public CsvInputIterator( Iterator<DataFactory> source, Header.Factory headerFactory,
            IdType idType, Configuration config, Collector badCollector )
    {
        this.source = source;
        this.headerFactory = headerFactory;
        this.idType = idType;
        this.config = config;
        this.badCollector = badCollector;
    }

    @Override
    public CsvInputChunk newChunk()
    {
        return new CsvInputChunk( idType, config.delimiter(), badCollector, extractors(),
                new ProcessingChunk( new char[config.bufferSize()] ) );
    }

    private Extractors extractors()
    {
        return new Extractors( config.arrayDelimiter() );
    }

    @Override
    public synchronized boolean next( InputChunk chunk ) throws IOException
    {
        while ( true )
        {
            if ( current == null )
            {
                if ( !source.hasNext() )
                {
                    return false;
                }
                current = new Single( source.next() );
            }

            if ( current.next( chunk ) )
            {
                return true;
            }
            current.close();
            current = null;
        }
    }

    private CharSeeker seeker( Chunk chunk )
    {
        return new BufferedCharSeeker( Source.singleChunk( chunk ), config );
    }

    @Override
    public void close()
    {
        try
        {
            if ( current != null )
            {
                current.close();
            }
            current = null;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public String sourceDescription()
    {
        return null;
    }

    @Override
    public long lineNumber()
    {
        return 0;
    }

    @Override
    public long position()
    {
        return 0;
    }

    private class Single
    {
        private final CharReadableChunker chunker;
        private final Header header;
        private boolean firstReturned;
        private final CharSeeker firstSeeker;
        private final Decorator decorator;

        public Single( DataFactory dataFactory ) throws IOException
        {
            Data data = dataFactory.create( config );
            CharReadable stream = data.stream();
            decorator = data.decorator();

            chunker = new CharReadableChunker( stream, config.bufferSize() );
            ProcessingChunk firstChunk = chunker.newChunk();
            chunker.nextChunk( firstChunk );
            firstSeeker = seeker( firstChunk );
            header = headerFactory.create( firstSeeker, config, idType );
        }

        public boolean next( InputChunk chunk ) throws IOException
        {
            // TODO Decoration has to happen in here!

            if ( !firstReturned )
            {
                firstReturned = true;
                return initialized( chunk, firstSeeker );
            }

            CsvInputChunk csvChunk = (CsvInputChunk) chunk;
            ProcessingChunk processingChunk = csvChunk.processingChunk();
            if ( chunker.nextChunk( processingChunk ) )
            {
                return initialized( chunk, seeker( processingChunk ) );
            }
            return false;
        }

        private boolean initialized( InputChunk chunk, CharSeeker seeker )
        {
            CsvInputChunk csvChunk = (CsvInputChunk) chunk;
            csvChunk.initialize( seeker, header.clone(), decorator );
            return true;
        }

        public void close() throws IOException
        {
            chunker.close();
        }
    }
}
