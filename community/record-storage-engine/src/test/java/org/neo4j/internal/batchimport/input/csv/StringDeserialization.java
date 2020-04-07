/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.batchimport.input.csv;

import java.lang.reflect.Array;
import java.util.function.Function;

import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.SourceTraceability;

/**
 * {@link Deserialization} that writes the values to a {@link StringBuilder}, suitable for piping the
 * data straight down to a .csv file.
 */
public class StringDeserialization implements Deserialization<String>
{
    private final StringBuilder builder = new StringBuilder();
    private final Configuration config;
    private int field;

    public StringDeserialization( Configuration config )
    {
        this.config = config;
    }

    @Override
    public void handle( Header.Entry entry, Object value )
    {
        if ( field > 0 )
        {
            builder.append( config.delimiter() );
        }
        if ( value != null )
        {
            stringify( value );
        }
        field++;
    }

    private void stringify( Object value )
    {
        if ( value instanceof String )
        {
            String string = (String) value;
            boolean quote = string.indexOf( '.' ) != -1 || string.indexOf( config.quotationCharacter() ) != -1;
            if ( quote )
            {
                builder.append( config.quotationCharacter() );
            }
            builder.append( string );
            if ( quote )
            {
                builder.append( config.quotationCharacter() );
            }
        }
        else if ( value.getClass().isArray() )
        {
            int length = Array.getLength( value );
            for ( int i = 0; i < length; i++ )
            {
                Object item = Array.get( value, i );
                if ( i > 0 )
                {
                    builder.append( config.arrayDelimiter() );
                }
                stringify( item );
            }
        }
        else if ( value instanceof Number )
        {
            Number number = (Number) value;
            if ( value instanceof Float )
            {
                builder.append( number.floatValue() );
            }
            else if ( value instanceof Double )
            {
                builder.append( number.doubleValue() );
            }
            else
            {
                builder.append( number.longValue() );
            }
        }
        else if ( value instanceof Boolean )
        {
            builder.append( ((Boolean) value ).booleanValue() );
        }
        else
        {
            throw new IllegalArgumentException( value + " " + value.getClass().getSimpleName() );
        }
    }

    @Override
    public String materialize()
    {
        return builder.toString();
    }

    @Override
    public void clear()
    {
        builder.delete( 0, builder.length() );
        field = 0;
    }

    public static Function<SourceTraceability,Deserialization<String>> factory( final Configuration config )
    {
        return from -> new StringDeserialization( config );
    }
}
