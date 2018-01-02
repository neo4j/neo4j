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
package org.neo4j.tooling;

import java.lang.reflect.Array;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.function.Function;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Deserialization;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

/**
 * {@link Deserialization} that writes the values to a {@link StringBuilder}, suitable for piping the
 * data straight down to a .csv file.
 */
class StringDeserialization implements Deserialization<String>
{
    private final StringBuilder builder = new StringBuilder();
    private final Configuration config;

    StringDeserialization( Configuration config )
    {
        this.config = config;
    }

    @Override
    public void initialize()
    {   // Do nothing
    }

    @Override
    public void handle( Entry entry, Object value )
    {
        if ( builder.length() > 0 )
        {
            builder.append( config.delimiter() );
        }
        stringify( entry, value );
    }

    private void stringify( Entry entry, Object value )
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
                stringify( entry, item );
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
            throw new IllegalArgumentException( value.toString() + " " + value.getClass().getSimpleName() );
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
    }

    public static Function<SourceTraceability,Deserialization<String>> factory( final Configuration config )
    {
        return new Function<SourceTraceability,Deserialization<String>>()
        {
            @Override
            public Deserialization<String> apply( SourceTraceability from ) throws RuntimeException
            {
                return new StringDeserialization( config );
            }
        };
    }
}
