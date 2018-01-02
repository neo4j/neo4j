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

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.function.Function;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.UnexpectedEndOfInputException;

import static java.lang.String.format;

/**
 * Converts a line of csv data into an {@link InputEntity} (either a node or relationship).
 * Does so by seeking values, using {@link CharSeeker}, interpreting the values using a {@link Header}.
 */
public class InputEntityDeserializer<ENTITY extends InputEntity>
        extends PrefetchingIterator<ENTITY> implements InputIterator<ENTITY>
{
    private final Header header;
    private final CharSeeker data;
    private final Mark mark = new Mark();
    private final int delimiter;
    private final Function<ENTITY,ENTITY> decorator;
    private final Deserialization<ENTITY> deserialization;
    private final Validator<ENTITY> validator;
    private final Extractors.StringExtractor stringExtractor = new Extractors.StringExtractor( false );
    private final Collector badCollector;

    InputEntityDeserializer( Header header, CharSeeker data, int delimiter,
            Deserialization<ENTITY> deserialization, Function<ENTITY,ENTITY> decorator,
            Validator<ENTITY> validator, Collector badCollector )
    {
        this.header = header;
        this.data = data;
        this.delimiter = delimiter;
        this.deserialization = deserialization;
        this.decorator = decorator;
        this.validator = validator;
        this.badCollector = badCollector;
    }

    public void initialize()
    {
        deserialization.initialize();
    }

    @Override
    protected ENTITY fetchNextOrNull()
    {
        // Read a CSV "line" and convert the values into what they semantically mean.
        try
        {
            if ( !deserializeNextFromSource() )
            {
                return null;
            }

            // When we have everything, create an input entity out of it
            ENTITY entity = deserialization.materialize();

            // Ignore additional values on this, but log it in case user doesn't realise that the header specifies
            // less columns than the data. Prints in close() so it only happens once per file.
            while ( !mark.isEndOfLine() )
            {
                long lineNumber = data.lineNumber();
                data.seek( mark, delimiter );
                data.tryExtract( mark, stringExtractor );
                badCollector.collectExtraColumns(
                        data.sourceDescription(), lineNumber, stringExtractor.value() );
            }

            entity = decorator.apply( entity );
            validator.validate( entity );

            return entity;
        }
        catch ( IOException e )
        {
            throw new InputException( "Unable to read more data from input stream", e );
        }
        finally
        {
            deserialization.clear();
        }
    }

    private boolean deserializeNextFromSource() throws IOException
    {
        Header.Entry[] entries = header.entries();
        if ( entries.length == 0 )
        {
            return false;
        }
        int fieldIndex = 0;
        try
        {
            for ( ; fieldIndex < entries.length; fieldIndex++ )
            {
                // Seek the next value
                if ( !data.seek( mark, delimiter ) )
                {
                    if ( fieldIndex > 0 )
                    {
                        throw new UnexpectedEndOfInputException( "Near " + mark );
                    }
                    // We're just at the end
                    return false;
                }

                // Extract it, type according to our header
                Header.Entry entry = entries[fieldIndex];
                if ( entry.type() != Type.IGNORE )
                {
                    Object value = data.tryExtract( mark, entry.extractor() )
                            ? entry.extractor().value() : null;
                    deserialization.handle( entry, value );
                }

                if ( mark.isEndOfLine() )
                {   // We're at the end of the line, break and return an entity with what we have.
                    break;
                }
            }
            return true;
        }
        catch ( final RuntimeException e )
        {
            String stringValue = null;
            try
            {
                Extractors extractors = new Extractors( '?' );
                if ( data.tryExtract( mark, extractors.string() ) )
                {
                    stringValue = extractors.string().value();
                }
            }
            catch ( Exception e1 )
            {   // OK
            }

            String message = format( "ERROR in input" +
                    "%n  data source: %s" +
                    "%n  in field: %s" +
                    "%n  for header: %s" +
                    "%n  raw field value: %s" +
                    "%n  original error: %s",
                    data, entries[fieldIndex] + ":" + (fieldIndex+1), header,
                    stringValue != null ? stringValue : "??",
                    e.getMessage() );
            if ( e instanceof InputException )
            {
                throw Exceptions.withMessage( e, message );
            }
            throw new InputException( message, e );
        }
    }

    @Override
    public void close()
    {
        try
        {
            data.close();
        }
        catch ( IOException e )
        {
            throw new InputException( "Unable to close data iterator", e );
        }
    }

    @Override
    public long position()
    {
        return data.position();
    }

    @Override
    public String sourceDescription()
    {
        return data.sourceDescription();
    }

    @Override
    public long lineNumber()
    {
        return data.lineNumber();
    }
}
