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

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.csv.reader.BufferedCharSeeker;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharReadableChunker;
import org.neo4j.csv.reader.CharReadableChunker.ChunkImpl;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.ClosestNewLineChunker;
import org.neo4j.csv.reader.Readables;
import org.neo4j.csv.reader.Source;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;

import static java.util.Arrays.copyOf;

class CsvInputIterator implements SourceTraceability, Closeable
{
    private final CharReadable stream;
    private final CharReadableChunker chunker;
    private final Header header;
    private final Decorator decorator;
    private final Configuration config;

    CsvInputIterator( CharReadable stream, Decorator decorator, Header header, Configuration config )
    {
        if ( config.multilineFields() )
        {
            throw new UnsupportedOperationException( "Multi-line fields unsupported" );
        }

        this.stream = stream;
        this.config = config;
        this.decorator = decorator;
        this.header = header;
        this.chunker = new ClosestNewLineChunker( stream, config.bufferSize() );
    }

    CsvInputIterator( CharReadable stream, Decorator decorator, Header.Factory headerFactory, IdType idType,
            Configuration config, Groups groups ) throws IOException
    {
        this( stream, decorator, extractHeader( stream, headerFactory, idType, config, groups ), config );
    }

    static Header extractHeader( CharReadable stream, Header.Factory headerFactory, IdType idType,
            Configuration config, Groups groups ) throws IOException
    {
        if ( !headerFactory.isDefined() )
        {
            char[] firstLineBuffer = Readables.extractFirstLineFrom( stream );
            // make the chunk slightly bigger than the header to not have the seeker think that it's reading
            // a value bigger than its max buffer size
            ChunkImpl firstChunk = new ChunkImpl( copyOf( firstLineBuffer, firstLineBuffer.length + 1 ) );
            firstChunk.initialize( firstLineBuffer.length, stream.sourceDescription() );
            CharSeeker firstSeeker = seeker( firstChunk, config );
            return headerFactory.create( firstSeeker, config, idType, groups );
        }
        else
        {
            // TODO: blargh
            return headerFactory.create( null, null, null, null );
        }
    }

    public boolean next( InputChunk chunk ) throws IOException
    {
        CsvInputChunk csvChunk = (CsvInputChunk) chunk;
        Chunk processingChunk = csvChunk.processingChunk();
        if ( chunker.nextChunk( processingChunk ) )
        {
            return initialized( chunk, seeker( processingChunk, config ) );
        }
        return false;
    }

    private boolean initialized( InputChunk chunk, CharSeeker seeker )
    {
        CsvInputChunk csvChunk = (CsvInputChunk) chunk;
        return csvChunk.initialize( seeker, header.clone(), decorator );
    }

    @Override
    public void close() throws IOException
    {
        chunker.close();
    }

    @Override
    public String sourceDescription()
    {
        return stream.sourceDescription();
    }

    @Override
    public long position()
    {
        return stream.position();
    }

    private static CharSeeker seeker( Chunk chunk, Configuration config )
    {
        return new BufferedCharSeeker( Source.singleChunk( chunk ), config );
    }
}
