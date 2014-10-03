/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.unsafe.impl.batchimport.input.DuplicateHeaderException;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.MissingHeaderException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.BufferedCharSeeker;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.CharSeeker;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.Extractor;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.Extractors;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.Mark;

import static org.neo4j.unsafe.impl.batchimport.input.csv.reader.BufferedCharSeeker.DEFAULT_BUFFER_SIZE;
import static org.neo4j.unsafe.impl.batchimport.input.csv.reader.QuoteAwareCharSeeker.quoteAware;
import static org.neo4j.unsafe.impl.batchimport.input.csv.reader.ThreadAheadReadable.threadAhead;

/**
 * Provides common implementations of factories required by f.ex {@link CsvInput}.
 */
public class DataFactories
{
    /**
     * @return a {@link CharSeeker} over the supplied {@code file}.
     */
    public static DataFactory file( final File file )
    {
        return new DataFactory()
        {
            @SuppressWarnings( "resource" )
            @Override
            public CharSeeker create( Configuration config )
            {
                try
                {
                    // Reader for the file
                    Readable reader = new FileReader( file );

                    // Thread that always has one buffer read ahead
                    reader = threadAhead( reader, DEFAULT_BUFFER_SIZE );

                    // Give the reader to the char seeker
                    CharSeeker result = new BufferedCharSeeker( reader, DEFAULT_BUFFER_SIZE );

                    // If we so desire make it quote aware
                    if ( config.quoteAware() )
                    {
                        result = quoteAware( result, config.quotationCharacter() );
                    }
                    return result;
                }
                catch ( FileNotFoundException e )
                {
                    throw new InputException( e.getMessage(), e );
                }
            }
        };
    }

    public static Header.Factory defaultFormatNodeFileHeader()
    {
        return new AbstractFileHeaderParser( Type.ID )
        {
            @Override
            protected Header.Entry entry( int index, String name, String typeSpec, Extractors extractors,
                    Extractor<?> idExtractor )
            {
                // For nodes it's simply ID,LABEL,PROPERTY. typeSpec can be either ID,LABEL or a type of property,
                // like 'int' or 'string_array' or similar, or empty for 'string' property.
                Type type = null;
                Extractor<?> extractor = null;
                if ( name.trim().length() == 0 )
                {
                    type = Type.IGNORE;
                }
                else if ( typeSpec == null )
                {
                    type = Type.PROPERTY;
                    extractor = Extractors.STRING;
                }
                else if ( typeSpec.equalsIgnoreCase( Type.ID.name() ) )
                {
                    type = Type.ID;
                    extractor = idExtractor;
                }
                else if ( typeSpec.equalsIgnoreCase( Type.LABEL.name() ) )
                {
                    type = Type.LABEL;
                    extractor = extractors.stringArray();
                }
                else
                {
                    type = Type.PROPERTY;
                    extractor = extractors.valueOf( typeSpec );
                }

                return new Header.Entry( name, type, extractor );
            }
        };
    }

    public static Header.Factory defaultFormatRelationshipFileHeader()
    {
        return new AbstractFileHeaderParser( Type.START_NODE, Type.END_NODE, Type.RELATIONSHIP_TYPE )
        {
            @Override
            protected Header.Entry entry( int index, String name, String typeSpec, Extractors extractors,
                    Extractor<?> idExtractor )
            {
                Type type = null;
                Extractor<?> extractor = null;
                if ( index == 0 )
                {
                    type = Type.START_NODE;
                    extractor = idExtractor;
                }
                else if ( index == 1 )
                {
                    type = Type.END_NODE;
                    extractor = idExtractor;
                }
                else if ( index == 2 )
                {
                    type = Type.RELATIONSHIP_TYPE;
                    extractor = Extractors.STRING;
                }
                else
                {   // Property
                    type = Type.PROPERTY;
                    extractor = extractors.valueOf( typeSpec );
                }

                return new Header.Entry( name, type, extractor );
            }
        };
    }

    private static abstract class AbstractFileHeaderParser implements Header.Factory
    {
        private final Type[] mandatoryTypes;

        protected AbstractFileHeaderParser( Type... mandatoryTypes )
        {
            this.mandatoryTypes = mandatoryTypes;
        }

        @Override
        public Header create( CharSeeker seeker, Configuration config, Extractor<?> idExtractor )
        {
            try
            {
                Mark mark = new Mark();
                Extractors extractors = new Extractors( config.arrayDelimiter() );
                int[] delimiter = new int[] {config.delimiter()};
                List<Header.Entry> columns = new ArrayList<>();
                for ( int i = 0; !mark.isEndOfLine() && seeker.seek( mark, delimiter ); i++ )
                {
                    String columnString = seeker.extract( mark, Extractors.STRING );
                    int typeIndex = columnString.lastIndexOf( ':' );
                    String name;
                    String typeSpec;
                    if ( typeIndex != -1 )
                    {   // Specific type given
                        name = columnString.substring( 0, typeIndex );
                        typeSpec = columnString.substring( typeIndex+1 );
                    }
                    else
                    {
                        name = columnString;
                        typeSpec = null;
                    }
                    columns.add( entry( i, name, typeSpec, extractors, idExtractor ) );
                }
                Entry[] entries = columns.toArray( new Header.Entry[columns.size()] );
                validateHeader( entries );
                return new Header( entries );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        private void validateHeader( Entry[] entries )
        {
            Map<String,Entry> properties = new HashMap<>();
            Map<Type,Entry> singletonEntries = new HashMap<>();
            for ( Entry entry : entries )
            {
                switch ( entry.type() )
                {
                case PROPERTY:
                    Entry existingPropertyEntry = properties.get( entry.name() );
                    if ( existingPropertyEntry != null )
                    {
                        throw new DuplicateHeaderException( existingPropertyEntry, entry );
                    }
                    properties.put( entry.name(), entry );
                    break;

                case ID: case START_NODE: case END_NODE: case RELATIONSHIP_TYPE:
                    Entry existingSingletonEntry = singletonEntries.get( entry.type() );
                    if ( existingSingletonEntry != null )
                    {
                        throw new DuplicateHeaderException( existingSingletonEntry, entry );
                    }
                    singletonEntries.put( entry.type(), entry );
                    break;
                }
            }

            for ( Type type : mandatoryTypes )
            {
                if ( !singletonEntries.containsKey( type ) )
                {
                    throw new MissingHeaderException( type );
                }
            }
        }

        /**
         * @param idExtractor we supply the id extractor explicitly because it's a configuration,
         * or at least input-global concern and not a concern of this particular header.
         */
        protected abstract Header.Entry entry( int index, String name, String typeSpec, Extractors extractors,
                Extractor<?> idExtractor );
    }
}
