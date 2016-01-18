/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public class Converters
{
    public static <T> Function<String,T> mandatory()
    {
        return key -> {
            throw new IllegalArgumentException( "Missing argument '" + key + "'" );
        };
    }

    public static <T> Function<String,T> optional()
    {
        return from -> null;
    }

    public static <T> Function<String,T> withDefault( final T defaultValue )
    {
        return from -> defaultValue;
    }

    public static Function<String,File> toFile()
    {
        return File::new;
    }

    public static final Comparator<File> BY_FILE_NAME = ( o1, o2 ) -> o1.getName().compareTo( o2.getName() );

    public static final Comparator<File> BY_FILE_NAME_WITH_CLEVER_NUMBERS =
            ( o1, o2 ) -> NumberAwareStringComparator.INSTANCE.compare( o1.getAbsolutePath(), o2.getAbsolutePath() );

    public static Function<String,File[]> regexFiles( final boolean cleverNumberRegexSort )
    {
        return name -> {
            Comparator<File> sorting = cleverNumberRegexSort ? BY_FILE_NAME_WITH_CLEVER_NUMBERS : BY_FILE_NAME;
            List<File> files = Validators.matchingFiles( new File( name ) );
            Collections.sort( files, sorting );
            return files.toArray( new File[files.size()] );
        };
    }

    public static Function<String,File[]> toFiles( final String delimiter,
            final Function<String,File[]> eachFileConverter )
    {
        return from -> {
            if ( from == null )
            {
                return new File[0];
            }

            String[] names = from.split( delimiter );
            List<File> files = new ArrayList<>();
            for ( String name : names )
            {
                for ( File file : eachFileConverter.apply( name ) )
                {
                    files.add( file );
                }
            }
            return files.toArray( new File[files.size()] );
        };
    }

    public static Function<String,Integer> toInt()
    {
        return Integer::new;
    }
}
