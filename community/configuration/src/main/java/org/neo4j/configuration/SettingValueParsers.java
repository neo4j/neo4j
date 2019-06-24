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
package org.neo4j.configuration;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.DatabaseNameValidator;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.TimeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.string.SecureString;
import org.neo4j.values.storable.DateTimeValue;

import static java.lang.Character.isDigit;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.io.fs.FileUtils.fixSeparatorsInPath;
import static org.neo4j.util.Preconditions.checkArgument;

public final class SettingValueParsers
{
    private SettingValueParsers()
    {
    }

    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String LIST_SEPARATOR = ",";

    // Pre defined parses
    public static SettingValueParser<String> STRING = new SettingValueParser<>()
    {
        @Override
        public String parse( String value )
        {
            return value.trim();
        }

        @Override
        public String getDescription()
        {
            return "a string";
        }
    };

    public static SettingValueParser<SecureString> SECURE_STRING = new SettingValueParser<>()
    {
        @Override
        public SecureString parse( String value )
        {
            return new SecureString( value.trim() );
        }

        @Override
        public String getDescription()
        {
            return "a secure string";
        }
    };

    public static SettingValueParser<Integer> INT = new SettingValueParser<>()
    {
        @Override
        public Integer parse( String value )
        {
            try
            {
                return Integer.parseInt( value );
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( "not a valid integer value", e );
            }
        }

        @Override
        public String getDescription()
        {
            return "an integer";
        }
    };

    public static SettingValueParser<Long> LONG = new SettingValueParser<>()
    {
        @Override
        public Long parse( String value )
        {
            try
            {
                return Long.parseLong( value );
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( "not a valid long value", e );
            }
        }

        @Override
        public String getDescription()
        {
            return "a long";
        }
    };

    public static SettingValueParser<Boolean> BOOL = new SettingValueParser<>()
    {
        @Override
        public Boolean parse( String value )
        {
            if ( value.equalsIgnoreCase( "true" ) )
            {
                return Boolean.TRUE;
            }
            else if ( value.equalsIgnoreCase( "false" ) )
            {
                return Boolean.FALSE;
            }
            else
            {
                throw new IllegalArgumentException( "must be 'true' or 'false'" );
            }
        }

        @Override
        public String getDescription()
        {
            return "a boolean";
        }
    };

    public static SettingValueParser<Double> DOUBLE = new SettingValueParser<>()
    {
        @Override
        public Double parse( String value )
        {
            try
            {
                return Double.parseDouble( value );
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( "not a valid double value", e );
            }
        }

        @Override
        public String getDescription()
        {
            return "a double";
        }
    };

    public static <T> SettingValueParser<List<T>> listOf( SettingValueParser<T> parser )
    {
        return new SettingValueParser<>()
        {
            @Override
            public List<T> parse( String value )
            {
                return Arrays.stream( value.split( LIST_SEPARATOR ) )
                        .map( String::trim )
                        .filter( StringUtils::isNotEmpty )
                        .map( parser::parse )
                        .collect( Collectors.toList() );
            }

            @Override
            public String valueToString( List<T> value )
            {
                return StringUtils.join( value, LIST_SEPARATOR );
            }

            @Override
            public String getDescription()
            {
                return format( "a '%s' separated list with elements of type '%s'.", LIST_SEPARATOR, parser.getDescription() );
            }
        };
    }

    public static <T extends Enum<T>> SettingValueParser<T> ofEnum( final Class<T> enumClass )
    {
        return internalEnum( EnumSet.allOf( enumClass ) );
    }

    /** An ENUM parser accepting a subset of the ENUM values
     *
     * @param values a subset of valid ENUM values to be accepted.
     * @param <T> concrete type of the enum.
     * @return a SettingValueParser parsing only provided values.
     */
    @SafeVarargs
    public static <T extends Enum<T>> SettingValueParser<T> ofPartialEnum( T... values )
    {
        return internalEnum( Arrays.asList( values ) );
    }

    private static <T extends Enum<T>> SettingValueParser<T> internalEnum( final Collection<T> values )
    {
        return new SettingValueParser<>()
        {
            @Override
            public T parse( String value )
            {
                for ( T t : values )
                {
                    if ( t.toString().equalsIgnoreCase( value ) )
                    {
                        return t;
                    }
                }

                throw new IllegalArgumentException( format( "%s not one of %s", value, values.toString() ) );
            }

            @Override
            public String getDescription()
            {
                return "one of " + values.toString();
            }
        };
    }

    public static SettingValueParser<HostnamePort> HOSTNAME_PORT = new SettingValueParser<>()
    {
        @Override
        public HostnamePort parse( String value )
        {
            return new HostnamePort( value );
        }

        @Override
        public String getDescription()
        {
            return "a hostname and port";
        }
    };

    public static SettingValueParser<Duration> DURATION = new SettingValueParser<>()
    {
        @Override
        public Duration parse( String value )
        {
            return Duration.ofMillis( TimeUtil.parseTimeMillis.apply( value ) );
        }

        @Override
        public String getDescription()
        {
            return "a duration (" + TimeUtil.VALID_TIME_DESCRIPTION + ")";
        }

        @Override
        public String valueToString( Duration value )
        {
            return TimeUtil.nanosToString( value.toNanos() );
        }
    };

    public static SettingValueParser<ZoneId> TIMEZONE = new SettingValueParser<>()
    {

        @Override
        public ZoneId parse( String value )
        {
            try
            {
                return DateTimeValue.parseZoneOffsetOrZoneName( value );
            }
            catch ( Exception e )
            {
                throw new IllegalArgumentException( "Not a valid timezone value", e );
            }
        }

        @Override
        public String getDescription()
        {
            return "a string describing a timezone, either described by offset (e.g. '+02:00') or by name (e.g. 'Europe/Stockholm')";
        }
    };

    public static SettingValueParser<SocketAddress> SOCKET_ADDRESS = new SettingValueParser<>()
    {
        @Override
        public SocketAddress parse( String value )
        {
            return SocketAddressParser.socketAddress( value , SocketAddress::new );
        }

        @Override
        public String getDescription()
        {
            return "a socket address";
        }

        @Override
        public  SocketAddress solveDependency( SocketAddress value, SocketAddress dependencyValue )
        {
            return solve( value, dependencyValue );
        }

        @Override
        public SocketAddress solveDefault( SocketAddress value, SocketAddress defaultValue )
        {
            return value != null ? solve( value, defaultValue ) : null;
        }

        private SocketAddress solve( SocketAddress value, SocketAddress dependencyValue )
        {
            if ( value == null )
            {
                return dependencyValue;
            }
            String hostname = value.getHostname();
            int port = value.getPort();
            if ( dependencyValue != null )
            {
                if ( StringUtils.isEmpty( hostname ) )
                {
                    hostname = dependencyValue.getHostname();
                }
                if ( port < 0 )
                {
                    port = dependencyValue.getPort();
                }
            }

            return new SocketAddress( hostname, port );
        }
    };

    public static SettingValueParser<Long> BYTES = new SettingValueParser<>()
    {
        private static final String VALID_MULTIPLIERS = "`k`, `m`, `g`, `K`, `M`, `G`";
        @Override
        public Long parse( String value )
        {
            long bytes;
            try
            {
                bytes = ByteUnit.parse( value );
            }
            catch ( IllegalArgumentException e )
            {
                throw new IllegalArgumentException( format( "%s is not a valid size, must be e.g. 10, 5K, 1M, 11G", value ) );
            }
            if ( bytes < 0 )
            {
                throw new IllegalArgumentException( value + " is not a valid number of bytes. Must be positive or zero." );
            }
            return bytes;
        }

        @Override
        public String getDescription()
        {
            return format("a byte size (valid multipliers are %s)", VALID_MULTIPLIERS );
        }
    };

    public static SettingValueParser<URI> URI = new SettingValueParser<>()
    {
        @Override
        public URI parse( String value )
        {
            try
            {
                return new URI( value );
            }
            catch ( URISyntaxException e )
            {
                throw new IllegalArgumentException( "not a valid URI" );
            }
        }

        @Override
        public String getDescription()
        {
            return "a URI";
        }
    };

    public static SettingValueParser<URI> NORMALIZED_RELATIVE_URI = new SettingValueParser<>()
    {
        @Override
        public URI parse( String value )
        {
            try
            {
                String normalizedUri = new URI( value ).normalize().getPath();
                if ( normalizedUri.endsWith( "/" ) )
                {
                    // Force the string end without "/"
                    normalizedUri = normalizedUri.substring( 0, normalizedUri.length() - 1 );
                }
                return new URI( normalizedUri );
            }
            catch ( URISyntaxException e )
            {
                throw new IllegalArgumentException( "not a valid URI" );
            }
        }

        @Override
        public String getDescription()
        {
            return "a normalized relative URI";
        }
    };

    public static SettingValueParser<Path> PATH = new SettingValueParser<>()
    {
        @Override
        public Path parse( String value )
        {
            return Path.of( fixSeparatorsInPath( value ) ).normalize();
        }

        @Override
        public String getDescription()
        {
            return "a path";
        }

        @Override
        public Path solveDependency( Path value, Path dependencyValue )
        {
            requireNonNull( dependencyValue, "Dependency can not be null" );
            checkArgument( dependencyValue.isAbsolute(), "Dependency must be absolute path" );

            if ( value != null )
            {
                if ( value.isAbsolute() )
                {
                    return value;
                }
                return dependencyValue.resolve( value );
            }
            return dependencyValue;
        }
    };

    public static SettingValueParser<String> DATABASENAME = new SettingValueParser<>()
    {
        @Override
        public String parse( String name )
        {
            DatabaseNameValidator.assertValidDatabaseName( new NormalizedDatabaseName( name ) );
            return name;
        }

        @Override
        public String getDescription()
        {
            return "A valid database name";
        }
    };

    public static long parseLongWithUnit( String numberWithPotentialUnit )
    {
        int firstNonDigitIndex = findFirstNonDigit( numberWithPotentialUnit );
        String number = numberWithPotentialUnit.substring( 0, firstNonDigitIndex );

        long multiplier = 1;
        if ( firstNonDigitIndex < numberWithPotentialUnit.length() )
        {
            String unit = numberWithPotentialUnit.substring( firstNonDigitIndex );
            if ( unit.equalsIgnoreCase( "k" ) )
            {
                multiplier = 1024;
            }
            else if ( unit.equalsIgnoreCase( "m" ) )
            {
                multiplier = 1024 * 1024;
            }
            else if ( unit.equalsIgnoreCase( "g" ) )
            {
                multiplier = 1024 * 1024 * 1024;
            }
            else
            {
                throw new IllegalArgumentException(
                        "Illegal unit '" + unit + "' for number '" + numberWithPotentialUnit + "'" );
            }
        }

        return parseLong( number ) * multiplier;
    }

    /**
     * @return index of first non-digit character in {@code numberWithPotentialUnit}. If all digits then
     * {@code numberWithPotentialUnit.length()} is returned.
     */
    private static int findFirstNonDigit( String numberWithPotentialUnit )
    {
        int firstNonDigitIndex = numberWithPotentialUnit.length();
        for ( int i = 0; i < numberWithPotentialUnit.length(); i++ )
        {
            if ( !isDigit( numberWithPotentialUnit.charAt( i ) ) )
            {
                firstNonDigitIndex = i;
                break;
            }
        }
        return firstNonDigitIndex;
    }

}
