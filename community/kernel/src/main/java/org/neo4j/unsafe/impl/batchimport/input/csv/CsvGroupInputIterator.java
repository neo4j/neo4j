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

import org.neo4j.csv.reader.CharReadableChunker.ChunkImpl;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.MultiReadable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;

/**
 * Iterates over one group of input data, e.g. one or more input files. A whole group conforms to the same header.
 */
public class CsvGroupInputIterator extends InputIterator.Adapter
{
    private final Iterator<DataFactory> source;
    private final Header.Factory headerFactory;
    private final IdType idType;
    private final Configuration config;
    private final Collector badCollector;
    private final Groups groups;
    private CsvInputIterator current;

    public CsvGroupInputIterator( Iterator<DataFactory> source, Header.Factory headerFactory,
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
    public InputChunk newChunk()
    {
        return config.multilineFields()
                ? new EagerlyReadInputChunk()
                : new CsvInputChunk( idType, config.delimiter(), badCollector, extractors( config ),
                        new ChunkImpl( new char[config.bufferSize()] ) );
    }

    static Extractors extractors( Configuration config )
    {
        return new Extractors( config.arrayDelimiter(), config.emptyQuotedStringsAsNull() );
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
                Data data = source.next().create( config );
                current = new CsvInputIterator( new MultiReadable( data.stream() ), data.decorator(),
                        headerFactory, idType, config, groups, badCollector, extractors( config ) );
            }

            if ( current.next( chunk ) )
            {
                return true;
            }
            current.close();
            current = null;
        }
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
}
