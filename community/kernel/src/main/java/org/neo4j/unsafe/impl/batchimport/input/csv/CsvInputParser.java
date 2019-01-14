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
import java.lang.reflect.Array;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.csv.reader.Extractors.LongExtractor;
import org.neo4j.helpers.Exceptions;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.UnexpectedEndOfInputException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

import static java.lang.String.format;

/**
 * CSV data to input entity parsing logic. Parsed CSV data is fed into {@link InputEntityVisitor}.
 */
public class CsvInputParser implements Closeable
{
    private final CharSeeker seeker;
    private final Mark mark = new Mark();
    private final IdType idType;
    private final Header header;
    private final int delimiter;
    private final Collector badCollector;
    private final Extractor<String> stringExtractor;

    private long lineNumber;

    public CsvInputParser( CharSeeker seeker, int delimiter, IdType idType, Header header,
            Collector badCollector, Extractors extractors )
    {
        this.seeker = seeker;
        this.delimiter = delimiter;
        this.idType = idType;
        this.header = header;
        this.badCollector = badCollector;
        this.stringExtractor = extractors.string();
    }

    boolean next( InputEntityVisitor visitor ) throws IOException
    {
        lineNumber++;
        int i = 0;
        Entry entry = null;
        Entry[] entries = header.entries();
        try
        {
            boolean doContinue = true;
            for ( i = 0; i < entries.length && doContinue; i++ )
            {
                entry = entries[i];
                if ( !seeker.seek( mark, delimiter ) )
                {
                    if ( i > 0 )
                    {
                        throw new UnexpectedEndOfInputException( "Near " + mark );
                    }
                    // We're just at the end
                    return false;
                }

                switch ( entry.type() )
                {
                case ID:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        switch ( idType )
                        {
                        case STRING:
                        case INTEGER:
                            Object idValue = entry.extractor().value();
                            doContinue = visitor.id( idValue, entry.group() );
                            if ( entry.name() != null )
                            {
                                doContinue = visitor.property( entry.name(), idValue );
                            }
                            break;
                        case ACTUAL:
                            doContinue = visitor.id( ((LongExtractor) entry.extractor()).longValue() );
                            break;
                        default: throw new IllegalArgumentException( idType.name() );
                        }
                    }
                    break;
                case START_ID:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        switch ( idType )
                        {
                        case STRING:
                            doContinue = visitor.startId( entry.extractor().value(), entry.group() );
                            break;
                        case INTEGER:
                            doContinue = visitor.startId( entry.extractor().value(), entry.group() );
                            break;
                        case ACTUAL:
                            doContinue = visitor.startId( ((LongExtractor) entry.extractor()).longValue() );
                            break;
                        default: throw new IllegalArgumentException( idType.name() );
                        }
                    }
                    break;
                case END_ID:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        switch ( idType )
                        {
                        case STRING:
                            doContinue = visitor.endId( entry.extractor().value(), entry.group() );
                            break;
                        case INTEGER:
                            doContinue = visitor.endId( entry.extractor().value(), entry.group() );
                            break;
                        case ACTUAL:
                            doContinue = visitor.endId( ((LongExtractor) entry.extractor()).longValue() );
                            break;
                        default: throw new IllegalArgumentException( idType.name() );
                        }
                    }
                    break;
                 case TYPE:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        doContinue = visitor.type( (String) entry.extractor().value() );
                    }
                    break;
                case PROPERTY:
                    if ( seeker.tryExtract( mark, entry.extractor(), entry.optionalParameter() ) )
                    {
                        // TODO since PropertyStore#encodeValue takes Object there's no point splitting up
                        // into different primitive types
                        Object value = entry.extractor().value();
                        if ( !isEmptyArray( value ) )
                        {
                            doContinue = visitor.property( entry.name(), value );
                        }
                    }
                    break;
                case LABEL:
                    if ( seeker.tryExtract( mark, entry.extractor() ) )
                    {
                        Object labelsValue = entry.extractor().value();
                        if ( labelsValue.getClass().isArray() )
                        {
                            doContinue = visitor.labels( (String[]) labelsValue );
                        }
                        else
                        {
                            doContinue = visitor.labels( new String[] {(String) labelsValue} );
                        }
                    }
                    break;
                case IGNORE:
                    break;
                default:
                    throw new IllegalArgumentException( entry.type().toString() );
                }

                if ( mark.isEndOfLine() )
                {
                    // We're at the end of the line, break and return an entity with what we have.
                    break;
                }
            }

            while ( !mark.isEndOfLine() )
            {
                seeker.seek( mark, delimiter );
                if ( doContinue )
                {
                    seeker.tryExtract( mark, stringExtractor, entry.optionalParameter() );
                    badCollector.collectExtraColumns(
                            seeker.sourceDescription(), lineNumber, stringExtractor.value() );
                }
            }
            visitor.endOfEntity();
            return true;
        }
        catch ( final RuntimeException e )
        {
            String stringValue = null;
            try
            {
                Extractors extractors = new Extractors( '?' );
                if ( seeker.tryExtract( mark, extractors.string(), entry.optionalParameter() ) )
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
                    seeker, entry + ":" + (i + 1), header,
                    stringValue != null ? stringValue : "??",
                    e.getMessage() );

            if ( e instanceof InputException )
            {
                throw Exceptions.withMessage( e, message );
            }
            throw new InputException( message, e );
        }
    }

    private static boolean isEmptyArray( Object value )
    {
        return value.getClass().isArray() && Array.getLength( value ) == 0;
    }

    @Override
    public void close() throws IOException
    {
        seeker.close();
    }
}
