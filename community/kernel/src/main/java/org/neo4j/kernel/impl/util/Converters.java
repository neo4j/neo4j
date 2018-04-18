/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.Pair;

import static org.neo4j.function.Predicates.not;

public class Converters
{
    private Converters()
    {
    }

    public static <T> Function<String,T> mandatory()
    {
        return key ->
        {
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

    public static Function<String, Path> toPath()
    {
        return Paths::get;
    }

    public static Function<String, String> identity()
    {
        return s -> s;
    }

    public static final Comparator<File> BY_FILE_NAME = Comparator.comparing( File::getName );

    public static final Comparator<File> BY_FILE_NAME_WITH_CLEVER_NUMBERS =
            ( o1, o2 ) -> NumberAwareStringComparator.INSTANCE.compare( o1.getAbsolutePath(), o2.getAbsolutePath() );

    public static Function<String,File[]> regexFiles( final boolean cleverNumberRegexSort )
    {
        return name ->
        {
            Comparator<File> sorting = cleverNumberRegexSort ? BY_FILE_NAME_WITH_CLEVER_NUMBERS : BY_FILE_NAME;
            List<File> files = Validators.matchingFiles( new File( name ) );
            files.sort( sorting );
            return files.toArray( new File[files.size()] );
        };
    }

    public static Function<String,File[]> toFiles( final String delimiter,
            final Function<String,File[]> eachFileConverter )
    {
        return from ->
        {
            if ( from == null )
            {
                return new File[0];
            }

            String[] names = from.split( delimiter );
            List<File> files = new ArrayList<>();
            for ( String name : names )
            {
                files.addAll( Arrays.asList( eachFileConverter.apply( name ) ) );
            }
            return files.toArray( new File[files.size()] );
        };
    }

    public static Function<String,Integer> toInt()
    {
        return Integer::new;
    }

    /**
     * Takes a raw address that can have a single port or 2 ports (lower and upper bounds of port range) and
     * processes it to a clean separation of host and ports. When only one port is specified, it is in the lower bound.
     * The presence of an upper bound implies a range.
     *
     * @param rawAddress the raw address that a user can provide via config or command line
     * @return the host, lower bound port, and upper bound port
     */
    public static OptionalHostnamePort toOptionalHostnamePortFromRawAddress( String rawAddress )
    {
        Pair<Optional<Integer>,String> firstPair = popPort( rawAddress );
        Pair<Optional<Integer>,String> secondPair = popPort( firstPair.other() );

        boolean twoPorts = secondPair.first().isPresent();
        Integer lowerBound = null;
        Integer upperBound = null;
        if ( twoPorts )
        {
            lowerBound = secondPair.first().get();
            upperBound = firstPair.first().get();
        }
        else if ( firstPair.first().isPresent() )
        {
            lowerBound = firstPair.first().get();
        }
        String address = Stream.of( secondPair.other() )
                .map( Converters::removeIpv6Brackets )
                .filter( not( String::isEmpty ) )
                .findFirst()
                .orElse( null );
        return new OptionalHostnamePort( address, lowerBound, upperBound );
    }

    /**
     * IPv6 addresses with ports can be wrapped in brackets. This method assumes the ports have been removed from the raw
     * address and what remains is a IPv6 address that is wrapped in brackets. It will return the address without the brackets.
     *
     * @param rawAddress a potential ipv6 address within brackets. Non-ipv6 addresses will work as well, but will be ignored since they cannot be in brackets
     * @return the address without brackets
     */
    private static String removeIpv6Brackets( String rawAddress )
    {
        Pattern pattern = Pattern.compile( "^\\[(?<address>.+)\\]$" );
        Matcher matcher = pattern.matcher( rawAddress );
        if ( matcher.find() )
        {
            return matcher.group( "address" );
        }
        return rawAddress;
    }

    /**
     * Given an address it will 'pop' the port that is attached to the end of the address
     * and clean at most 1 colon that is used as a delimiter between host (or port) and port.
     *
     * @param rawAddress the raw address such as "neo4j.com:123:456"
     * @return a pair representing a possible port that was found and whatever remains from removing that port from the address
     */
    private static Pair<Optional<Integer>,String> popPort( String rawAddress )
    {
        Pattern suffixPortPattern = Pattern.compile( ":(?<port>\\d+)$" );
        Matcher matcher = suffixPortPattern.matcher( rawAddress );
        if ( matcher.find() )
        {
            Integer port = Integer.parseInt( matcher.group( "port" ) );
            int length = rawAddress.length() - (port.toString().length() + 1); // colon
            String remainingAddress = rawAddress.substring( 0, length );
            return Pair.of( Optional.of( port ), remainingAddress );
        }
        String address = rawAddress;
        if ( rawAddress.endsWith( ":" ) )
        {
            address = rawAddress.substring( 0, rawAddress.length() - 1 );
        }
        return Pair.of( Optional.empty(), address );
    }
}
