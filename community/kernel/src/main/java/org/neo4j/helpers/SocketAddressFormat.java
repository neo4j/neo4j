/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.helpers;

import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public class SocketAddressFormat
{
    private static final Pattern hostnamePortPattern = Pattern.compile( "(?<hostname>[^\\s]+):(?<port>\\d+)" );
    private static final Pattern portPattern = Pattern.compile( ":(?<port>\\d+)" );

    public static <T extends SocketAddress> T socketAddress(
            String name, String value, String defaultHostname, int defaultPort,
            BiFunction<String, Integer, T> constructor )
    {
        String hostname = defaultHostname;
        int port = defaultPort;

        if ( value != null )
        {
            String trimmedValue = value.trim();
            Matcher hostnamePortMatcher = hostnamePortPattern.matcher( trimmedValue );
            Matcher portMatcher = portPattern.matcher( trimmedValue );
            if ( hostnamePortMatcher.matches() )
            {
                hostname = hostnamePortMatcher.group( "hostname" );
                port = parseInt( hostnamePortMatcher.group( "port" ) );
            }
            else if ( portMatcher.matches() )
            {
                port = parseInt( portMatcher.group( "port" ) );
            }
            else
            {
                throw new IllegalArgumentException( format( "Setting \"%s\" must be in the format " +
                        "\"hostname:port\" or \":port\". \"%s\" does not conform to these formats", name, value ) );
            }
        }

        return constructor.apply( hostname, port );
    }

    public static <T extends SocketAddress> T socketAddress( String value,
                                                             BiFunction<String, Integer, T> constructor )
    {
        if ( value == null )
        {
            throw new IllegalArgumentException( "Cannot parse socket address from null" );
        }
        String trimmedValue = value.trim();
        Matcher hostnamePortMatcher = hostnamePortPattern.matcher( trimmedValue );
        if ( hostnamePortMatcher.matches() )
        {
            String hostname = hostnamePortMatcher.group( "hostname" );
            int port = parseInt( hostnamePortMatcher.group( "port" ) );
            return constructor.apply( hostname, port );
        }
        else
        {
            throw new IllegalArgumentException( format( "Configured socket address must be in the format " +
                    "\"hostname:port\". \"%s\" does not conform to this format", value ) );
        }
    }
}
