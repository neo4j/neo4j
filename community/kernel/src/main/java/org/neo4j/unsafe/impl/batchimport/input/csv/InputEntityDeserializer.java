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

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Mark;
import org.neo4j.function.Function;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.UnexpectedEndOfInputException;

import static java.util.Arrays.copyOf;

/**
 * Converts a line of csv data into an {@link InputEntity} (either a node or relationship).
 * Does so by seeking values, using {@link CharSeeker}, interpreting the values using a {@link Header}.
 */
abstract class InputEntityDeserializer<ENTITY extends InputEntity> extends PrefetchingResourceIterator<ENTITY>
{
    private final Header header;
    private final CharSeeker data;
    private final Mark mark = new Mark();
    private final int[] delimiter;
    private final Function<ENTITY,ENTITY> decorator;

    // Data
    // holder of properties, alternating key/value. Will grow with the entity having most properties.
    private Object[] properties = new Object[10*2];
    private int propertiesCursor;

    InputEntityDeserializer( Header header, CharSeeker data, int[] delimiter, Function<ENTITY,ENTITY> decorator )
    {
        this.header = header;
        this.data = data;
        this.delimiter = delimiter;
        this.decorator = decorator;
    }

    @Override
    protected ENTITY fetchNextOrNull()
    {
        // Read a CSV "line" and convert the values into what they semantically mean.
        try
        {
            Header.Entry[] entries = header.entries();
            for ( int i = 0; i < entries.length; i++ )
            {
                // Seek the next value
                if ( !data.seek( mark, delimiter ) )
                {
                    if ( i > 0 )
                    {
                        throw new UnexpectedEndOfInputException( "Near " + mark );
                    }
                    // We're just at the end
                    return null;
                }

                // Extract it, type according to our header
                Header.Entry entry = entries[i];
                Object value = data.extract( mark, entry.extractor() ).value();
                boolean handled = true;
                switch ( entry.type() )
                {
                case PROPERTY:
                    ensurePropertiesArrayCapacity( propertiesCursor+2 );
                    properties[propertiesCursor++] = entry.name();
                    properties[propertiesCursor++] = value;
                    break;
                case IGNORE: // value ignored
                    break;
                default:
                    handled = false;
                    break;
                }

                // This is an abstract base class, so send on to sub classes if we didn't know
                // how to handle a header entry of this type
                if ( !handled )
                {
                    handleValue( entry, value );
                }

                if ( mark.isEndOfLine() )
                {   // We're at the end of the line, break and return an entity with what we have.
                    break;
                }
            }

            // When we have everything, create an input entity out of it
            ENTITY entity = convertToInputEntity( properties() );

            // If there are more values on this line, ignore them
            // TODO perhaps log about them?
            while ( !mark.isEndOfLine() )
            {
                data.seek( mark, delimiter );
            }

            return decorator.apply( entity );
        }
        catch ( IOException e )
        {
            throw new InputException( "Unable to read more data from input stream", e );
        }
        finally
        {
            propertiesCursor = 0;
        }
    }

    private Object[] properties()
    {
        return propertiesCursor > 0
                ? copyOf( properties, propertiesCursor )
                : InputEntity.NO_PROPERTIES;
    }

    private void ensurePropertiesArrayCapacity( int length )
    {
        if ( length > properties.length )
        {
            properties = Arrays.copyOf( properties, length );
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

    protected abstract ENTITY convertToInputEntity( Object[] properties );

    protected abstract void handleValue( Header.Entry entry, Object value );
}
