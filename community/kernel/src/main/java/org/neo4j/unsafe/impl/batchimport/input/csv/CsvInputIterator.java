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
import org.neo4j.csv.reader.CharReadableChunker.ChunkImpl;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.ClosestNewLineChunker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.MultiLineAwareChunker;
import org.neo4j.csv.reader.Readables;
import org.neo4j.csv.reader.Source;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;

public class CsvInputIterator extends InputIterator.Adapter
{
    private final Iterator<DataFactory> source;
    private final Header.Factory headerFactory;
    private final IdType idType;
    private final Configuration config;
    private final Collector badCollector;
    private final Groups groups;
    private Single current;

    public CsvInputIterator( Iterator<DataFactory> source, Header.Factory headerFactory,
            IdType idType, Configuration config, Collector badCollector, Groups groups )
    {
        this.source = source;
        this.headerFactory = headerFactory;
        this.idType = idType;
        this.config = config;
        this.badCollector = badCollector;
        this.groups = groups;
    }

    @Override
    public CsvInputChunk newChunk()
    {
        return new CsvInputChunk( idType, config.delimiter(), badCollector, extractors(),
                new ChunkImpl( new char[config.bufferSize()] ) );
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

    private class Single
    {
        private final CharReadableChunker chunker;
        private final Header header;
        private boolean firstReturned;
        private final CharSeeker firstSeeker;
        private final Decorator decorator;

        Single( DataFactory dataFactory ) throws IOException
        {
            Data data = dataFactory.create( config );
            CharReadable stream = data.stream();
            char[] firstLineBuffer = Readables.extractFirstLineFrom( stream );
            ChunkImpl firstChunk = new ChunkImpl( firstLineBuffer );
            firstChunk.initialize( firstLineBuffer.length, stream.sourceDescription() );
            firstSeeker = seeker( firstChunk );
            header = headerFactory.create( firstSeeker, config, idType, groups );

            // Since we don't know whether or not the header is supplied or extracted
            // (it's abstracted in HeaderFactory) we have to treat this first line as the first chunk anyway
            // and potentially it will contain no data except the header, but that's fine.

            decorator = data.decorator();
            chunker = config.multilineFields()
                    ? new MultiLineAwareChunker( stream, config, header.entries().length, config.delimiter() )
                    : new ClosestNewLineChunker( stream, config.bufferSize() );
        }

        public boolean next( InputChunk chunk ) throws IOException
        {
            if ( !firstReturned )
            {
                firstReturned = true;
                return initialized( chunk, firstSeeker );
            }

            CsvInputChunk csvChunk = (CsvInputChunk) chunk;
            Chunk processingChunk = csvChunk.processingChunk();
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
