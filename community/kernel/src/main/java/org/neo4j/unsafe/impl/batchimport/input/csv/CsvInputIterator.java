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

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.csv.reader.BufferedCharSeeker;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharReadableChunker.ChunkImpl;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Chunker;
import org.neo4j.csv.reader.ClosestNewLineChunker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Readables;
import org.neo4j.csv.reader.Source;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import static java.util.Arrays.copyOf;
import static org.neo4j.unsafe.impl.batchimport.input.csv.CsvGroupInputIterator.extractors;

/**
 * Iterates over one stream of data, where all data items conform to the same {@link Header}.
 * Typically created from {@link CsvGroupInputIterator}.
 */
class CsvInputIterator implements SourceTraceability, Closeable
{
    private final CharReadable stream;
    private final Chunker chunker;
    private final int groupId;
    private final Decorator decorator;
    private final Supplier<CsvInputChunk> realInputChunkSupplier;

    CsvInputIterator( CharReadable stream, Decorator decorator, Header header, Configuration config, IdType idType, Collector badCollector,
            Extractors extractors, int groupId )
    {
        this.stream = stream;
        this.decorator = decorator;
        this.groupId = groupId;
        if ( config.multilineFields() )
        {
            // If we're expecting multi-line fields then there's no way to arbitrarily chunk the underlying data source
            // and find record delimiters with certainty. This is why we opt for a chunker that does parsing inside
            // the call that normally just hands out an arbitrary amount of characters to parse outside and in parallel.
            // This chunker is single-threaded, as it was previously too and keeps the functionality of multi-line fields.
            this.chunker = new EagerParserChunker( stream, idType, header, badCollector, extractors, 1_000, config, decorator );
            this.realInputChunkSupplier = EagerCsvInputChunk::new;
        }
        else
        {
            this.chunker = new ClosestNewLineChunker( stream, config.bufferSize() );
            this.realInputChunkSupplier = () -> new LazyCsvInputChunk( idType, config.delimiter(), badCollector,
                    extractors( config ), chunker.newChunk(), config, decorator, header );
        }
    }

    CsvInputIterator( CharReadable stream, Decorator decorator, Header.Factory headerFactory, IdType idType, Configuration config, Groups groups,
            Collector badCollector, Extractors extractors, int groupId ) throws IOException
    {
        this( stream, decorator, extractHeader( stream, headerFactory, idType, config, groups ), config, idType, badCollector, extractors, groupId );
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

        return headerFactory.create( null, null, null, null );
    }

    public boolean next( CsvInputChunkProxy proxy ) throws IOException
    {
        proxy.ensureInstantiated( realInputChunkSupplier, groupId );
        return proxy.fillFrom( chunker );
    }

    @Override
    public void close() throws IOException
    {
        chunker.close();
        decorator.close();
    }

    @Override
    public String sourceDescription()
    {
        return stream.sourceDescription();
    }

    @Override
    public long position()
    {
        return chunker.position();
    }

    static CharSeeker seeker( Chunk chunk, Configuration config )
    {
        return new BufferedCharSeeker( Source.singleChunk( chunk ), config );
    }
}
