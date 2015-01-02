/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.io.File;

import org.neo4j.function.Function;

public class Converters
{
    public static <T> Function<String,T> mandatory()
    {
        return new Function<String,T>()
        {
            @Override
            public T apply( String key )
            {
                throw new IllegalArgumentException( "Missing argument '" + key + "'" );
            }
        };
    }

    public static <T> Function<String,T> optional()
    {
        return new Function<String,T>()
        {
            @Override
            public T apply( String from )
            {
                return null;
            }
        };
    }

    public static <T> Function<String,T> withDefault( final T defaultValue )
    {
        return new Function<String,T>()
        {
            @Override
            public T apply( String from )
            {
                return defaultValue;
            }
        };
    }

    public static Function<String,File> toFile()
    {
        return new Function<String,File>()
        {
            @Override
            public File apply( String from )
            {
                return new File( from );
            }
        };
    }

    public static Function<String,File[]> toFiles( final String delimiter )
    {
        return new Function<String,File[]>()
        {
            @Override
            public File[] apply( String from )
            {
                String[] names = from.split( delimiter );
                File[] file = new File[names.length];
                for ( int i = 0; i < names.length; i++ )
                {
                    file[i] = new File( names[i] );
                }
                return file;
            }
        };
    }

    public static Function<String,Character> toCharacter()
    {
        return new Function<String,Character>()
        {
            @Override
            public Character apply( String value )
            {
                if ( value.length() > 1 )
                {
                    throw new IllegalArgumentException( "Invalid delimiter '" +
                            value + "', expected one character" );
                }
                return value.charAt( 0 );
            }
        };
    }

    public static Function<String,Integer> toInt()
    {
        return new Function<String,Integer>()
        {
            @Override
            public Integer apply( String from )
            {
                return new Integer( from );
            }
        };
    }
}
