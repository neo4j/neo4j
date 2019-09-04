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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.Numbers;

import static java.lang.String.format;

public final class SettingConstraints
{
    private SettingConstraints()
    {
    }

    public static SettingConstraint<String> except( String... forbiddenValues )
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( String value )
            {
                if ( StringUtils.isNotBlank( value ) )
                {
                    if ( ArrayUtils.contains( forbiddenValues, value ) )
                    {
                        throw new IllegalArgumentException( format( "not allowed value is: %s", value ) );
                    }
                }
            }

            @Override
            public String getDescription()
            {
                if ( forbiddenValues.length > 1 )
                {
                    return format( "is none of %s", Arrays.toString( forbiddenValues ) );
                }
                else if ( forbiddenValues.length == 1 )
                {
                    return format( "is not `%s`", forbiddenValues[0] );
                }
                return "";
            }
        };
    }

    public static SettingConstraint<String> matches( String regex, String description )
    {
        return new SettingConstraint<>()
        {
            private final String descMsg = StringUtils.isEmpty( description ) ? "" : format(" (%s)", description);
            private final Pattern pattern = Pattern.compile( regex );

            @Override
            public void validate( String value )
            {
                if ( !pattern.matcher( value ).matches() )
                {
                    throw new IllegalArgumentException( format("value does not match expression: `%s`%s", regex, descMsg  ) );
                }
            }

            @Override
            public String getDescription()
            {
                return format( "matches the pattern `%s`%s", regex, descMsg );
            }
        };
    }

    public static SettingConstraint<String> matches( String regex )
    {
        return matches( regex, null );
    }

    public static <T extends Comparable<T>> SettingConstraint<T> min( final T minValue )
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( T value )
            {
                if ( value == null )
                {
                    throw new IllegalArgumentException( "can not be null" );
                }

                if ( minValue.compareTo( value ) > 0 )
                {
                    throw new IllegalArgumentException( format( "minimum allowed value is %s", minValue ) );
                }
            }

            @Override
            public String getDescription()
            {
                return format( "is minimum `%s`", minValue );
            }
        };
    }

    public static <T extends Comparable<T>> SettingConstraint<T> max( final T maxValue )
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( T value )
            {
                if ( value == null )
                {
                    throw new IllegalArgumentException( "can not be null" );
                }

                if ( maxValue.compareTo( value ) < 0 )
                {
                    throw new IllegalArgumentException( format( "maximum allowed value is %s", maxValue ) );
                }
            }

            @Override
            public String getDescription()
            {
                return format( "is maximum `%s`", maxValue );
            }
        };
    }

    public static <T extends Comparable<T>> SettingConstraint<T> range( final T minValue, final T maxValue )
    {
        return new SettingConstraint<>()
        {
            private SettingConstraint<T> max = max( maxValue );
            private SettingConstraint<T> min = min( minValue );

            @Override
            public void validate( T value )
            {
                min.validate( value );
                max.validate( value );
            }

            @Override
            public String getDescription()
            {
                return format( "is in the range `%s` to `%s`", minValue, maxValue );
            }
        };
    }

    public static <T> SettingConstraint<T> is( final T expected )
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( T value )
            {
                if ( !Objects.equals( value, expected  ) )
                {
                    throw new IllegalArgumentException( format( "is not `%s`", expected ) );
                }
            }

            @Override
            public String getDescription()
            {
                return format( "is `%s`", expected );
            }
        };
    }

    @SafeVarargs
    public static <T> SettingConstraint<T> any( SettingConstraint<T> first, SettingConstraint<T>... rest )
    {
        return new SettingConstraint<>()
        {
            private final SettingConstraint<T>[] constraints = ArrayUtil.concat( first, rest );
            @Override
            public void validate( T value )
            {
                for ( SettingConstraint<T> constraint : constraints )
                {
                    try
                    {
                        constraint.validate( value );
                        return; // Only one constraint needs to pass for this to pass.
                    }
                    catch ( RuntimeException e )
                    {
                        // Ignore
                    }
                }
                throw new IllegalArgumentException( format( "does not fulfill any of %s", getDescription() ) );
            }

            @Override
            public String getDescription()
            {
                return Arrays.stream( constraints ).map( SettingConstraint::getDescription ).collect( Collectors.joining( " or " ));
            }
        };
    }

    public static final SettingConstraint<Long> POWER_OF_2 = new SettingConstraint<>()
    {
        @Override
        public void validate( Long value )
        {
            if ( value != null && !Numbers.isPowerOfTwo( value ) )
            {
                throw new IllegalArgumentException( "only power of 2 values allowed" );
            }
        }

        @Override
        public String getDescription()
        {
            return "is power of 2";
        }
    };

    public static final SettingConstraint<Integer> PORT = new SettingConstraint<>()
    {
        private final SettingConstraint<Integer> range = range( 0, 65535 );

        @Override
        public void validate( Integer value )
        {
            range.validate( value );
        }

        @Override
        public String getDescription()
        {
            return "is a valid port number";
        }
    };

    public static <T> SettingConstraint<List<T>> size( final int size )
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( List<T> value )
            {
                if ( value == null )
                {
                    throw new IllegalArgumentException( "can not be null" );
                }

                if ( value.size() != size )
                {
                    throw new IllegalArgumentException( format( "needs to be of size %s", size ) );
                }
            }

            @Override
            public String getDescription()
            {
                return format( "is of size `%s`", size );
            }
        };
    }

    public static final SettingConstraint<SocketAddress> HOSTNAME_ONLY = new SettingConstraint<>()
    {
        @Override
        public void validate( SocketAddress value )
        {
            if ( value == null )
            {
                throw new IllegalArgumentException( "can not be null" );
            }

            if ( value.getPort() >= 0 )
            {
                throw new IllegalArgumentException( "can not have a port" );
            }

            if ( StringUtils.isBlank( value.getHostname() ) )
            {
                throw new IllegalArgumentException( "needs not a hostname" );
            }

        }

        @Override
        public String getDescription()
        {
            return "has no specified port";
        }
    };

    public static final SettingConstraint<Path> ABSOLUTE_PATH = new SettingConstraint<>()
    {
        @Override
        public void validate( Path value )
        {
            if ( !value.isAbsolute() )
            {
                throw new IllegalArgumentException( format( "`%s` is not an absolute path.", value ) );
            }
        }

        @Override
        public String getDescription()
        {
            return "is absolute";
        }
    };
}
