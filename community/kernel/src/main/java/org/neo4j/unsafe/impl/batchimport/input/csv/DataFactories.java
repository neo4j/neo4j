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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Mark;
import org.neo4j.function.Factory;
import org.neo4j.function.Function;
import org.neo4j.function.Supplier;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.unsafe.impl.batchimport.input.DuplicateHeaderException;
import org.neo4j.unsafe.impl.batchimport.input.HeaderException;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.MissingHeaderException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

import static org.neo4j.csv.reader.CharSeekers.charSeeker;
import static org.neo4j.csv.reader.Readables.files;

/**
 * Provides common implementations of factories required by f.ex {@link CsvInput}.
 */
public class DataFactories
{
    /**
     * Creates a {@link DataFactory} where data exists in multiple files. If the first line of the first file is a header,
     * {@link #defaultFormatNodeFileHeader()} can be used to extract that.
     *
     * @return {@link DataFactory} that returns a {@link CharSeeker} over all the supplied {@code files}.
     */
    public static <ENTITY extends InputEntity> DataFactory<ENTITY> data( final Function<ENTITY,ENTITY> decorator,
            final Charset charset, final File... files )
    {
        if ( files.length == 0 )
        {
            throw new IllegalArgumentException( "No files specified" );
        }

        return new DataFactory<ENTITY>()
        {
            @Override
            public Data<ENTITY> create( final Configuration config )
            {
                return new Data<ENTITY>()
                {
                    @Override
                    public CharSeeker stream()
                    {
                        try
                        {
                            return charSeeker( files( charset, files ), config, true );
                        }
                        catch ( IOException e )
                        {
                            throw new InputException( e.getMessage(), e );
                        }
                    }

                    @Override
                    public Function<ENTITY,ENTITY> decorator()
                    {
                        return decorator;
                    }
                };
            }
        };
    }

    /**
     * @param readable we need to have this as a {@link Factory} since one data file may be opened and scanned
     * multiple times.
     * @return {@link DataFactory} that returns a {@link CharSeeker} over the supplied {@code readable}
     */
    public static <ENTITY extends InputEntity> DataFactory<ENTITY> data( final Function<ENTITY,ENTITY> decorator,
            final Supplier<CharReadable> readable )
    {
        return new DataFactory<ENTITY>()
        {
            @Override
            public Data<ENTITY> create( final Configuration config )
            {
                return new Data<ENTITY>()
                {
                    @Override
                    public CharSeeker stream()
                    {
                        return charSeeker( readable.get(), config, true );
                    }

                    @Override
                    public Function<ENTITY,ENTITY> decorator()
                    {
                        return decorator;
                    }
                };
            }
        };
    }

    /**
     * Header parser that will read header information, using the default node header format,
     * from the top of the data file.
     *
     * This header factory can be used even when the header exists in a separate file, if that file
     * is the first in the list of files supplied to {@link #data(File...)}.
     */
    public static Header.Factory defaultFormatNodeFileHeader()
    {
        return new DefaultNodeFileHeaderParser( READ_FROM_DATA_SEEKER );
    }

    /**
     * Header parser that will read header information, using the default node header format,
     * from a {@link Readable} containing that data.
     * @param reader {@link Readable} containing header data.
     */
    public static Header.Factory defaultFormatNodeFileHeader( CharReadable reader )
    {
        return new DefaultNodeFileHeaderParser( new HeaderFromSeparateReaderFactory( reader ) );
    }

    /**
     * Header parser that will read header information, using the default relationship header format,
     * from the top of the data file.
     *
     * This header factory can be used even when the header exists in a separate file, if that file
     * is the first in the list of files supplied to {@link #data(File...)}.
     */
    public static Header.Factory defaultFormatRelationshipFileHeader()
    {
        return new DefaultRelationshipFileHeaderParser( READ_FROM_DATA_SEEKER );
    }

    /**
     * Header parser that will read header information, using the default relationship header format,
     * from a {@link Readable} containing that data.
     * @param reader {@link Readable} containing header data.
     */
    public static Header.Factory defaultFormatRelationshipFileHeader( CharReadable reader )
    {
        return new DefaultRelationshipFileHeaderParser( new HeaderFromSeparateReaderFactory( reader ) );
    }

    /**
     * Provides {@link CharSeeker} to read and parse header information from.
     */
    private interface HeaderCharSeekerFactory
    {
        /**
         * @param seeker the {@link CharSeeker} for the data file, if that's what we want.
         * @param config
         * @return the {@link CharSeeker} to extract header information from.
         * @throws IOException if {@link CharSeeker} couldn't be provided.
         */
        CharSeeker open( CharSeeker seeker, Configuration config ) throws IOException;

        /**
         * Closes the header {@link CharSeeker}. Only close if {@link #open(CharSeeker, Configuration)} opens its own.
         * @param seeker {@link CharSeeker} returned from {@link #open(CharSeeker, Configuration)}.
         */
        void close( CharSeeker seeker );
    }

    /**
     * Just uses the provided {@link CharSeeker} containing the data itself.
     */
    private static final HeaderCharSeekerFactory READ_FROM_DATA_SEEKER = new HeaderCharSeekerFactory()
    {
        @Override
        public CharSeeker open( CharSeeker seeker, Configuration config )
        {
            return seeker;
        }

        @Override
        public void close( CharSeeker seeker )
        {   // Leave it open for data reading later
        }
    };

    private static abstract class SeparateHeaderReaderFactory implements HeaderCharSeekerFactory
    {
        @Override
        public void close( CharSeeker seeker )
        {
            try
            {
                seeker.close();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to close header reader", e );
            }
        }
    }

    private static class HeaderFromSeparateReaderFactory extends SeparateHeaderReaderFactory
    {
        private final CharReadable readable;

        HeaderFromSeparateReaderFactory( CharReadable readable )
        {
            this.readable = readable;
        }

        @Override
        public CharSeeker open( CharSeeker seeker, Configuration config ) throws IOException
        {
            return charSeeker( readable, config, true );
        }
    }

    private static abstract class AbstractDefaultFileHeaderParser implements Header.Factory
    {
        private final Type[] mandatoryTypes;
        private final HeaderCharSeekerFactory headerCharSeekerFactory;

        protected AbstractDefaultFileHeaderParser( HeaderCharSeekerFactory headerCharSeekerFactory,
                Type... mandatoryTypes )
        {
            this.headerCharSeekerFactory = headerCharSeekerFactory;
            this.mandatoryTypes = mandatoryTypes;
        }

        @Override
        public Header create( CharSeeker dataSeeker, Configuration config, IdType idType )
        {
            CharSeeker headerSeeker = null;
            try
            {
                headerSeeker = headerCharSeekerFactory.open( dataSeeker, config );
                Mark mark = new Mark();
                Extractors extractors = new Extractors( config.arrayDelimiter(), config.emptyQuotedStringsAsNull() );
                Extractor<?> idExtractor = idType.extractor( extractors );
                int delimiter = config.delimiter();
                List<Header.Entry> columns = new ArrayList<>();
                for ( int i = 0; !mark.isEndOfLine() && headerSeeker.seek( mark, delimiter ); i++ )
                {
                    String entryString = headerSeeker.tryExtract( mark, extractors.string() )
                            ? extractors.string().value() : null;
                    HeaderEntrySpec spec = new HeaderEntrySpec( entryString );

                    if ( (spec.name == null && spec.type == null) ||
                         (spec.type != null && spec.type.equals( Type.IGNORE.name() )) )
                    {
                        columns.add( new Header.Entry( null, Type.IGNORE, null, null ) );
                    }
                    else
                    {
                        columns.add( entry( i, spec.name, spec.type, spec.groupName, extractors, idExtractor ) );
                    }
                }
                Entry[] entries = columns.toArray( new Header.Entry[columns.size()] );
                validateHeader( entries );
                return new Header( entries );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                if ( headerSeeker != null )
                {
                    headerCharSeekerFactory.close( headerSeeker );
                }
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

                case ID: case START_ID: case END_ID: case TYPE:
                    Entry existingSingletonEntry = singletonEntries.get( entry.type() );
                    if ( existingSingletonEntry != null )
                    {
                        throw new DuplicateHeaderException( existingSingletonEntry, entry );
                    }
                    singletonEntries.put( entry.type(), entry );
                    break;
                default:
                    // No need to validate other headers
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

        protected boolean isRecognizedType( String typeSpec )
        {
            for ( Type type : Type.values() )
            {
                if ( type.name().equalsIgnoreCase( typeSpec ) )
                {
                    return true;
                }
            }
            return false;
        }

        /**
         * @param idExtractor we supply the id extractor explicitly because it's a configuration,
         * or at least input-global concern and not a concern of this particular header.
         */
        protected abstract Header.Entry entry( int index, String name, String typeSpec, String groupName,
                Extractors extractors, Extractor<?> idExtractor );
    }

    private static class HeaderEntrySpec
    {
        private final String name;
        private final String type;
        private final String groupName;

        HeaderEntrySpec( String rawHeaderField )
        {
            String name = rawHeaderField;
            String type = null;
            String groupName = null;

            int typeIndex;
            if ( rawHeaderField != null && (typeIndex = rawHeaderField.lastIndexOf( ':' )) != -1 )
            {   // Specific type given
                name = typeIndex > 0 ? rawHeaderField.substring( 0, typeIndex ) : null;
                type = rawHeaderField.substring( typeIndex+1 );
                int groupNameStartIndex = type.indexOf( '(' );
                if ( groupNameStartIndex != -1 )
                {   // Specific group given also
                    if ( !type.endsWith( ")" ) )
                    {
                        throw new IllegalArgumentException( "Group specification in '" + rawHeaderField +
                                "' is invalid, format expected to be 'name:TYPE(group)' " +
                                "where TYPE and (group) are optional" );
                    }
                    groupName = type.substring( groupNameStartIndex+1, type.length()-1 );
                    type = type.substring( 0, groupNameStartIndex );
                }
            }

            this.name = name;
            this.type = type;
            this.groupName = groupName;
        }
    }

    private static class DefaultNodeFileHeaderParser extends AbstractDefaultFileHeaderParser
    {
        public DefaultNodeFileHeaderParser( HeaderCharSeekerFactory headerCharSeekerFactory )
        {
            super( headerCharSeekerFactory );
        }

        @Override
        protected Header.Entry entry( int index, String name, String typeSpec, String groupName, Extractors extractors,
                Extractor<?> idExtractor )
        {
            // For nodes it's simply ID,LABEL,PROPERTY. typeSpec can be either ID,LABEL or a type of property,
            // like 'int' or 'string_array' or similar, or empty for 'string' property.
            Type type = null;
            Extractor<?> extractor = null;
            if ( typeSpec == null )
            {
                type = Type.PROPERTY;
                extractor = extractors.string();
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
            else if ( isRecognizedType( typeSpec ) )
            {
                throw new HeaderException( "Unexpected node header type '" + typeSpec + "'" );
            }
            else
            {
                type = Type.PROPERTY;
                extractor = extractors.valueOf( typeSpec );
            }

            return new Header.Entry( name, type, groupName, extractor );
        }
    }

    private static class DefaultRelationshipFileHeaderParser extends AbstractDefaultFileHeaderParser
    {
        protected DefaultRelationshipFileHeaderParser( HeaderCharSeekerFactory headerCharSeekerFactory )
        {
            // Don't have TYPE as mandatory since a decorator could provide that
            super( headerCharSeekerFactory, Type.START_ID, Type.END_ID );
        }

        @Override
        protected Header.Entry entry( int index, String name, String typeSpec, String groupName, Extractors extractors,
                Extractor<?> idExtractor )
        {
            Type type = null;
            Extractor<?> extractor = null;
            if ( typeSpec == null )
            {   // Property
                type = Type.PROPERTY;
                extractor = extractors.string();
            }
            else if ( typeSpec.equalsIgnoreCase( Type.START_ID.name() ) )
            {
                type = Type.START_ID;
                extractor = idExtractor;
            }
            else if ( typeSpec.equalsIgnoreCase( Type.END_ID.name() ) )
            {
                type = Type.END_ID;
                extractor = idExtractor;
            }
            else if ( typeSpec.equalsIgnoreCase( Type.TYPE.name() ) )
            {
                type = Type.TYPE;
                extractor = extractors.string();
            }
            else if ( isRecognizedType( typeSpec ) )
            {
                throw new HeaderException( "Unexpected relationship header type '" + typeSpec + "'" );
            }
            else
            {
                type = Type.PROPERTY;
                extractor = extractors.valueOf( typeSpec );
            }

            return new Header.Entry( name, type, groupName, extractor );
        }
    }

    @SafeVarargs
    public static Iterable<DataFactory<InputNode>> nodeData( DataFactory<InputNode>... factories )
    {
        return Iterables.iterable( factories );
    }

    @SafeVarargs
    public static Iterable<DataFactory<InputRelationship>> relationshipData( DataFactory<InputRelationship>... factories )
    {
        return Iterables.iterable( factories );
    }
}
