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
package org.neo4j.kernel.configuration;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.ScopeAwareSetting;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.SocketAddressParser;
import org.neo4j.helpers.TimeUtil;
import org.neo4j.helpers.collection.Iterables;

import static java.lang.Character.isDigit;
import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.io.fs.FileUtils.fixSeparatorsInPath;

/**
 * Create settings for configurations in Neo4j. See {@link org.neo4j.graphdb.factory.GraphDatabaseSettings} for
 * example.
 *
 * <p>Each setting has a name, a parser that converts a string to the type of the setting, a default value,
 * an optional inherited setting, and optional value converters/constraints.
 *
 * <p>A parser is a function that takes a string and converts to some type T. The parser may throw {@link
 * IllegalArgumentException} if it fails.
 *
 * <p>The default value is the string representation of what you want as default. Special constants NO_DEFAULT, which means
 * that you don't want any default value at all, can be used if no appropriate default value exists.
 */
public class Settings
{
    private static final String MATCHES_PATTERN_MESSAGE = "matches the pattern `%s`";

    private interface SettingHelper<T> extends Setting<T>
    {
        String lookup( Function<String, String> settings );

        String defaultLookup( Function<String, String> settings );
    }

    public static final String NO_DEFAULT = null;
    public static final String EMPTY = "";

    public static final String TRUE = "true";
    public static final String FALSE = "false";

    public static final String DEFAULT = "default";

    public static final String SEPARATOR = ",";

    private static final String SIZE_FORMAT = "\\d+[kmgKMG]?";

    private static final String SIZE_UNITS = Arrays.toString(
            SIZE_FORMAT.substring( SIZE_FORMAT.indexOf( '[' ) + 1,
                    SIZE_FORMAT.indexOf( ']' ) )
                    .toCharArray() )
            .replace( "[", "" )
            .replace( "]", "" );

    public static final String ANY = ".+";

    /**
     * Helper class to build a {@link Setting}. A setting always have a name, a parser and a default value.
     *
     * @param <T> The concrete type of the setting that is being build
     */
    public static final class SettingBuilder<T>
    {
        private final String name;
        private final Function<String,T> parser;
        private final String defaultValue;
        private Setting<T> inheritedSetting;
        private List<BiFunction<T, Function<String,String>,T>> valueConstraints;

        private SettingBuilder( @Nonnull final String name, @Nonnull final Function<String,T> parser, @Nullable final String defaultValue )
        {
            this.name = name;
            this.parser = parser;
            this.defaultValue = defaultValue;
        }

        /**
         * Setup a class to inherit from. Both the default value and the actual user supplied value will be inherited.
         * Limited to one parent, but chains are allowed and works as expected by going up on level until a valid value
         * is found.
         *
         * @param inheritedSetting the setting to inherit value and default value from.
         * @throws AssertionError if more than one inheritance is provided.
         */
        @Nonnull
        public SettingBuilder<T> inherits( @Nonnull final Setting<T> inheritedSetting )
        {
            // Make sure we only inherits from one other setting
            if ( this.inheritedSetting != null )
            {
                throw new AssertionError( "Can only inherit from one setting" );
            }

            this.inheritedSetting = inheritedSetting;
            return this;
        }

        /**
         * Add a constraint to this setting. If an error occurs, the constraint should throw {@link IllegalArgumentException}.
         * Constraints are allowed to modify values and they are applied in the order they are attached to the builder.
         *
         * @param constraint to add.
         */
        @Nonnull
        public SettingBuilder<T> constraint( @Nonnull final BiFunction<T, Function<String,String>,T> constraint )
        {
            if ( valueConstraints == null )
            {
                valueConstraints = new LinkedList<>(); // Must guarantee order
            }
            valueConstraints.add( constraint );
            return this;
        }

        @Nonnull
        public Setting<T> build()
        {
            BiFunction<String,Function<String, String>, String> valueLookup = named();
            BiFunction<String, Function<String, String>, String> defaultLookup = determineDefaultLookup( defaultValue, valueLookup );
            if ( inheritedSetting != null )
            {
                valueLookup = inheritedValue( valueLookup, inheritedSetting );
                defaultLookup = inheritedDefault( defaultLookup, inheritedSetting );
            }

            return new DefaultSetting<>( name, parser, valueLookup, defaultLookup, valueConstraints );
        }
    }

    /**
     * Constructs a {@link Setting} with a specified default value.
     *
     * @param name of the setting, e.g. "dbms.transaction.timeout".
     * @param parser that will convert the string representation to the concrete type T.
     * @param defaultValue the string representation of the default value.
     * @param <T> the concrete type of the setting.
     */
    @Nonnull
    public static <T> Setting<T> setting( @Nonnull final String name, @Nonnull final Function<String,T> parser,
            @Nullable final String defaultValue )
    {
        return new SettingBuilder<>( name, parser, defaultValue ).build();
    }

    /**
     * Start building a setting with default value set to {@link Settings#NO_DEFAULT}.
     *
     * @param name of the setting, e.g. "dbms.transaction.timeout".
     * @param parser that will convert the string representation to the concrete type T.
     * @param <T> the concrete type of the setting.
     */
    @Nonnull
    public static <T> SettingBuilder<T> buildSetting( @Nonnull final String name, @Nonnull final Function<String, T> parser )
    {
        return buildSetting( name, parser, NO_DEFAULT );
    }

    /**
     * Start building a setting with a specified default value.
     *
     * @param name of the setting, e.g. "dbms.transaction.timeout".
     * @param parser that will convert the string representation to the concrete type T.
     * @param defaultValue the string representation of the default value.
     * @param <T> the concrete type of the setting.
     */
    @Nonnull
    public static <T> SettingBuilder<T> buildSetting( @Nonnull final String name, @Nonnull final Function<String,T> parser,
            @Nullable final String defaultValue )
    {
        return new SettingBuilder<>( name, parser, defaultValue );
    }

    public static BiFunction<String,Function<String,String>,String> determineDefaultLookup( String defaultValue,
            BiFunction<String,Function<String,String>,String> valueLookup )
    {
        BiFunction<String,Function<String,String>,String> defaultLookup;
        if ( defaultValue != null )
        {
            defaultLookup = withDefault( defaultValue, valueLookup );
        }
        else
        {
            defaultLookup = ( n, from ) -> null;
        }
        return defaultLookup;
    }

    public static <OUT, IN1, IN2> Setting<OUT> derivedSetting( String name,
                                                               Setting<IN1> in1, Setting<IN2> in2,
                                                               BiFunction<IN1, IN2, OUT> derivation,
                                                               Function<String, OUT> overrideConverter )
    {
        // NOTE:
        // we do not scope the input settings here (indeed they might be shared...)
        // if needed we can add a configuration option to allow for it
        return new ScopeAwareSetting<OUT>()
        {
            @Override
            protected String provideName()
            {
                return name;
            }

            @Override
            public String getDefaultValue()
            {
                return NO_DEFAULT;
            }

            @Override
            public OUT from( Configuration config )
            {
                return config.get( this );
            }

            @Override
            public OUT apply( Function<String, String> config )
            {
                String override = config.apply( name() );
                if ( override != null )
                {
                    // Derived settings are intended not to be overridden and we should throw an exception here. However
                    // we temporarily need to allow the Desktop app to override the value of the derived setting
                    // unsupported.dbms.directories.database because we are not yet in a position to rework it to
                    // conform to the standard directory structure layout.
                    return overrideConverter.apply( override );
                }
                return derivation.apply( in1.apply( config ), in2.apply( config ) );
            }

            @Override
            public String valueDescription()
            {
                return in1.valueDescription();
            }
        };
    }

    public static <OUT, IN1> Setting<OUT> derivedSetting( String name,
                                                          Setting<IN1> in1,
                                                          Function<IN1, OUT> derivation,
                                                          Function<String,OUT> overrideConverter )
    {
        return new ScopeAwareSetting<OUT>()
        {
            @Override
            protected String provideName()
            {
                return name;
            }

            @Override
            public String getDefaultValue()
            {
                return NO_DEFAULT;
            }

            @Override
            public OUT from( Configuration config )
            {
                return config.get( this );
            }

            @Override
            public OUT apply( Function<String, String> config )
            {
                String override = config.apply( name() );
                if ( override != null )
                {
                    return overrideConverter.apply( override );
                }
                return derivation.apply( in1.apply( config ) );
            }

            @Override
            public String valueDescription()
            {
                return in1.valueDescription();
            }
        };
    }

    public static Setting<File> pathSetting( String name, String defaultValue )
    {
        return new ScopeAwareSetting<File>()
        {
            @Override
            protected String provideName()
            {
                return name;
            }

            @Override
            public String getDefaultValue()
            {
                return defaultValue;
            }

            @Override
            public File from( Configuration config )
            {
                return config.get( this );
            }

            @Override
            public File apply( Function<String, String> config )
            {
                String value = config.apply( name() );
                if ( value == null )
                {
                    value = defaultValue;
                }
                if ( value == null )
                {
                    return null;
                }

                String setting = fixSeparatorsInPath( value );
                File settingFile = new File( setting );

                if ( settingFile.isAbsolute() )
                {
                    return settingFile;
                }
                else
                {
                    return new File( GraphDatabaseSettings.neo4j_home.apply( config ), setting );
                }
            }

            @Override
            public String valueDescription()
            {
                return "A filesystem path; relative paths are resolved against the installation root, _<neo4j-home>_";
            }
        };
    }

    private static <T> BiFunction<String,Function<String, String>, String> inheritedValue(
            final BiFunction<String,Function<String,String>, String> lookup, final Setting<T> inheritedSetting )
    {
        return ( name, settings ) ->
        {
            String value = lookup.apply( name, settings );
            if ( value == null )
            {
                value = ((SettingHelper<T>) inheritedSetting).lookup( settings );
            }
            return value;
        };
    }

    private static <T> BiFunction<String,Function<String, String>, String> inheritedDefault(
            final BiFunction<String,Function<String,String>, String> lookup, final Setting<T> inheritedSetting )
    {
        return ( name, settings ) ->
        {
            String value = lookup.apply( name, settings );
            if ( value == null )
            {
                value = ((SettingHelper<T>) inheritedSetting).defaultLookup( settings );
            }
            return value;
        };
    }

    public static final Function<String, Integer> INTEGER = new Function<String, Integer>()
    {
        @Override
        public Integer apply( String value )
        {
            try
            {
                return parseInt( value );
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( "not a valid integer value" );
            }
        }

        @Override
        public String toString()
        {
            return "an integer";
        }
    };

    public static final Function<String, Long> LONG = new Function<String, Long>()
    {
        @Override
        public Long apply( String value )
        {
            try
            {
                return parseLong( value );
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( "not a valid long value" );
            }
        }

        @Override
        public String toString()
        {
            return "a long";
        }
    };

    public static final Function<String, Boolean> BOOLEAN = new Function<String, Boolean>()
    {
        @Override
        public Boolean apply( String value )
        {
            if ( value.equalsIgnoreCase( "true" ) )
            {
                return true;
            }
            else if ( value.equalsIgnoreCase( "false" ) )
            {
                return false;
            }
            else
            {
                throw new IllegalArgumentException( "must be 'true' or 'false'" );
            }
        }

        @Override
        public String toString()
        {
            return "a boolean";
        }
    };

    public static final Function<String, Float> FLOAT = new Function<String, Float>()
    {
        @Override
        public Float apply( String value )
        {
            try
            {
                return parseFloat( value );
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( "not a valid float value" );
            }
        }

        @Override
        public String toString()
        {
            return "a float";
        }
    };

    public static final Function<String, Double> DOUBLE = new Function<String, Double>()
    {
        @Override
        public Double apply( String value )
        {
            try
            {
                return parseDouble( value );
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( "not a valid double value" );
            }
        }

        @Override
        public String toString()
        {
            return "a double";
        }
    };

    public static final Function<String, String> STRING = new Function<String, String>()
    {
        @Override
        public String apply( String value )
        {
            return value.trim();
        }

        @Override
        public String toString()
        {
            return "a string";
        }
    };

    public static final Function<String,List<String>> STRING_LIST = list( SEPARATOR, STRING );

    public static final Function<String,HostnamePort> HOSTNAME_PORT = new Function<String, HostnamePort>()
    {
        @Override
        public HostnamePort apply( String value )
        {
            return new HostnamePort( value );
        }

        @Override
        public String toString()
        {
            return "a hostname and port";
        }
    };

    public static final Function<String,Duration> DURATION = new Function<String, Duration>()
    {
        @Override
        public Duration apply( String value )
        {
            return Duration.ofMillis( TimeUtil.parseTimeMillis.apply( value ) );
        }

        @Override
        public String toString()
        {
            return "a duration (" + TimeUtil.VALID_TIME_DESCRIPTION + ")";
        }
    };

    public static final Function<String, ListenSocketAddress> LISTEN_SOCKET_ADDRESS =
            new Function<String, ListenSocketAddress>()
            {
                @Override
                public ListenSocketAddress apply( String value )
                {
                    return SocketAddressParser.socketAddress( value, ListenSocketAddress::new );
                }

                @Override
                public String toString()
                {
                    return "a listen socket address";
                }
            };

    public static final Function<String, AdvertisedSocketAddress> ADVERTISED_SOCKET_ADDRESS =
            new Function<String, AdvertisedSocketAddress>()
            {
                @Override
                public AdvertisedSocketAddress apply( String value )
                {
                    return SocketAddressParser.socketAddress( value, AdvertisedSocketAddress::new );
                }

                @Override
                public String toString()
                {
                    return "an advertised socket address";
                }
            };

    public static BaseSetting<ListenSocketAddress> listenAddress( String name, int defaultPort )
    {
        return new ScopeAwareSetting<ListenSocketAddress>()
        {
            @Override
            protected String provideName()
            {
                return name;
            }

            @Override
            public String getDefaultValue()
            {
                return default_listen_address.getDefaultValue() + ":" + defaultPort;
            }

            @Override
            public ListenSocketAddress from( Configuration config )
            {
                return config.get( this );
            }

            @Override
            public ListenSocketAddress apply( Function<String, String> config )
            {
                String name = name();
                String value = config.apply( name );
                String hostname = default_listen_address.apply( config );

                return SocketAddressParser.deriveSocketAddress( name, value, hostname, defaultPort, ListenSocketAddress::new );
            }

            @Override
            public String valueDescription()
            {
                return LISTEN_SOCKET_ADDRESS.toString();
            }
        };
    }

    public static BaseSetting<AdvertisedSocketAddress> advertisedAddress( String name,
            Setting<ListenSocketAddress> listenAddressSetting )
    {
        return new ScopeAwareSetting<AdvertisedSocketAddress>()
        {
            @Override
            protected String provideName()
            {
                return name;
            }

            @Override
            public String getDefaultValue()
            {
                return default_advertised_address.getDefaultValue() + ":" +
                        LISTEN_SOCKET_ADDRESS.apply( listenAddressSetting.getDefaultValue() ).socketAddress().getPort();
            }

            @Override
            public AdvertisedSocketAddress from( Configuration config )
            {
                return config.get( this );
            }

            @Override
            public AdvertisedSocketAddress apply( Function<String, String> config )
            {
                ListenSocketAddress listenSocketAddress = listenAddressSetting.apply( config );
                String hostname = default_advertised_address.apply( config );
                int port = listenSocketAddress.socketAddress().getPort();

                String name = name();
                String value = config.apply( name );

                return SocketAddressParser.deriveSocketAddress( name, value, hostname, port, AdvertisedSocketAddress::new );
            }

            @Override
            public void withScope( Function<String,String> scopingRule )
            {
                super.withScope( scopingRule );
                listenAddressSetting.withScope( scopingRule );
            }

            @Override
            public String valueDescription()
            {
                return ADVERTISED_SOCKET_ADDRESS.toString();
            }
        };
    }

    public static final Function<String, Long> BYTES = new Function<String, Long>()
    {
        @Override
        public Long apply( String value )
        {
            try
            {
                String mem = value.toLowerCase();
                long multiplier = 1;
                if ( mem.endsWith( "k" ) )
                {
                    multiplier = 1024;
                    mem = mem.substring( 0, mem.length() - 1 );
                }
                else if ( mem.endsWith( "m" ) )
                {
                    multiplier = 1024 * 1024;
                    mem = mem.substring( 0, mem.length() - 1 );
                }
                else if ( mem.endsWith( "g" ) )
                {
                    multiplier = 1024 * 1024 * 1024;
                    mem = mem.substring( 0, mem.length() - 1 );
                }

                long bytes = parseLong( mem.trim() ) * multiplier;
                if ( bytes < 0 )
                {
                    throw new IllegalArgumentException(
                            value + " is not a valid number of bytes. Must be positive or zero." );
                }
                return bytes;
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( String.format( "%s is not a valid size, must be e.g. 10, 5K, 1M, " +
                        "11G", value ) );
            }
        }

        @Override
        public String toString()
        {
            return "a byte size (valid multipliers are `" + SIZE_UNITS.replace( ", ", "`, `" ) + "`)";
        }
    };

    public static final Function<String, URI> URI =
            new Function<String, URI>()
            {
                @Override
                public URI apply( String value )
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
                public String toString()
                {
                    return "a URI";
                }
            };

    public static final Function<String, URI> NORMALIZED_RELATIVE_URI = new Function<String, URI>()
    {
        @Override
        public URI apply( String value )
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
        public String toString()
        {
            return "a URI";
        }
    };

    public static final Function<String, File> PATH = new Function<String, File>()
    {
        @Override
        public File apply( String setting )
        {
            File file = new File( fixSeparatorsInPath( setting ) );
            if ( !file.isAbsolute() )
            {
                throw new IllegalArgumentException( "Paths must be absolute. Got " + file );
            }
            return file;
        }

        @Override
        public String toString()
        {
            return "a path";
        }
    };

    public static <T extends Enum<T>> Function<String, T> options( final Class<T> enumClass )
    {
        return options( EnumSet.allOf( enumClass ), false );
    }

    public static <T extends Enum<T>> Function<String, T> options( final Class<T> enumClass, boolean ignoreCase )
    {
        return options( EnumSet.allOf( enumClass ), ignoreCase );
    }

    public static <T> Function<String, T> options( T... optionValues )
    {
        return options( Iterables.iterable( optionValues ), false );
    }

    public static <T> Function<String, T> optionsIgnoreCase( T... optionValues )
    {
        return options( Iterables.iterable( optionValues ), true );
    }

    public static <T> Function<String, T> options( final Iterable<T> optionValues, final boolean ignoreCase )
    {
        return new Function<String, T>()
        {
            @Override
            public T apply( String value )
            {
                for ( T optionValue : optionValues )
                {
                    String allowedValue = optionValue.toString();

                    if ( allowedValue.equals( value ) || (ignoreCase && allowedValue.equalsIgnoreCase( value )) )
                    {
                        return optionValue;
                    }
                }
                String possibleValues = Iterables.asList( optionValues ).toString();
                throw new IllegalArgumentException(
                        "must be one of " + possibleValues + " case " + (ignoreCase ? "insensitive" : "sensitive") );
            }

            @Override
            public String toString()
            {
                return describeOneOf( optionValues );
            }
        };
    }

    /**
     *
     * @param optionValues iterable of objects with descriptive toString methods
     * @return a string describing possible values like "one of `X, Y, Z`"
     */
    @Nonnull
    public static String describeOneOf( @Nonnull Iterable optionValues )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "one of `" );
        String comma = "";
        for ( Object optionValue : optionValues )
        {
            builder.append( comma ).append( optionValue.toString() );
            comma = "`, `";
        }
        builder.append( "`" );
        return builder.toString();
    }

    public static <T> Function<String, List<T>> list( final String separator, final Function<String, T> itemParser )
    {
        return new Function<String, List<T>>()
        {
            @Override
            public List<T> apply( String value )
            {
                List<T> list = new ArrayList<>();
                String[] parts = value.split( separator );
                for ( String part : parts )
                {
                    part = part.trim();
                    if ( StringUtils.isNotEmpty( part ) )
                    {
                        list.add( itemParser.apply( part ) );
                    }
                }
                return list;
            }

            @Override
            public String toString()
            {
                return "a list separated by \"" + separator + "\" where items are " + itemParser.toString();
            }
        };
    }

    // Modifiers
    public static BiFunction<String, Function<String, String>, String> matches( final String regex )
    {
        final Pattern pattern = Pattern.compile( regex );

        return new BiFunction<String, Function<String, String>, String>()
        {
            @Override
            public String apply( String value, Function<String, String> settings )
            {
                if ( !pattern.matcher( value ).matches() )
                {
                    throw new IllegalArgumentException( "value does not match expression:" + regex );
                }

                return value;
            }

            @Override
            public String toString()
            {
                return String.format( MATCHES_PATTERN_MESSAGE, regex );
            }
        };
    }

    public static BiFunction<List<String>,Function<String,String>,List<String>> nonEmptyList =
            new BiFunction<List<String>,Function<String,String>,List<String>>()
            {
                @Override
                public List<String> apply( List<String> values, Function<String,String> settings )
                {
                    if ( values.isEmpty() )
                    {
                        throw new IllegalArgumentException( "setting must not be empty" );
                    }
                    return values;
                }

                @Override
                public String toString()
                {
                    return "non-empty list";
                }
            };

    public static BiFunction<List<String>,Function<String,String>,List<String>> matchesAny( final String regex )
    {
        final Pattern pattern = Pattern.compile( regex );

        return new BiFunction<List<String>,Function<String,String>,List<String>>()
        {
            @Override
            public List<String> apply( List<String> values, Function<String,String> settings )
            {
                for ( String value : values )
                {
                    if ( !pattern.matcher( value ).matches() )
                    {
                        throw new IllegalArgumentException( "value does not match expression:" + regex );
                    }
                }

                return values;
            }

            @Override
            public String toString()
            {
                return String.format( MATCHES_PATTERN_MESSAGE, regex );
            }
        };
    }

    public static <T extends Comparable<T>> BiFunction<T, Function<String, String>, T> min( final T min )
    {
        return new BiFunction<T, Function<String, String>, T>()
        {
            @Override
            public T apply( T value, Function<String, String> settings )
            {
                if ( value != null && value.compareTo( min ) < 0 )
                {
                    throw new IllegalArgumentException( String.format( "minimum allowed value is: %s", min ) );
                }
                return value;
            }

            @Override
            public String toString()
            {
                return "is minimum `" + min + "`";
            }
        };
    }

    public static <T extends Comparable<T>> BiFunction<T, Function<String, String>, T> max( final T max )
    {
        return new BiFunction<T, Function<String, String>, T>()
        {
            @Override
            public T apply( T value, Function<String, String> settings )
            {
                if ( value != null && value.compareTo( max ) > 0 )
                {
                    throw new IllegalArgumentException( String.format( "maximum allowed value is: %s", max ) );
                }
                return value;
            }

            @Override
            public String toString()
            {
                return "is maximum `" + max + "`";
            }
        };
    }

    public static <T extends Comparable<T>> BiFunction<T, Function<String, String>, T> range( final T min, final T max )
    {
        return new BiFunction<T, Function<String, String>, T>()
        {
            @Override
            public T apply( T from1, Function<String, String> from2 )
            {
                return min(min).apply( max(max).apply( from1, from2 ), from2 );
            }

            @Override
            public String toString()
            {
                return String.format( "is in the range `%s` to `%s`", min, max );
            }
        };
    }

    public static final BiFunction<Integer, Function<String, String>, Integer> port =
            illegalValueMessage( "must be a valid port number", range( 0, 65535 ) );

    public static <T> BiFunction<T, Function<String, String>, T> illegalValueMessage( final String message,
            final BiFunction<T,Function<String,String>,T> valueFunction )
    {
        return new BiFunction<T, Function<String, String>, T>()
        {
            @Override
            public T apply( T from1, Function<String, String> from2 )
            {
                try
                {
                    return valueFunction.apply( from1, from2 );
                }
                catch ( IllegalArgumentException e )
                {
                    throw new IllegalArgumentException( message );
                }
            }

            @Override
            public String toString()
            {
                String description = message;
                if ( valueFunction != null
                     && !String.format( MATCHES_PATTERN_MESSAGE, ANY ).equals(
                             valueFunction.toString() ) )
                {
                    description += " (" + valueFunction.toString() + ")";
                }
                return description;
            }
        };
    }

    // Setting converters and constraints
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

    // Setting helpers
    private static BiFunction<String,Function<String, String>, String> named()
    {
        return ( name, settings ) -> settings.apply( name );
    }

    private static BiFunction<String,Function<String,String>,String> withDefault( final String defaultValue,
            final BiFunction<String,Function<String,String>,String> lookup )
    {
        return ( name, settings ) ->
        {
            String value = lookup.apply( name, settings );
            if ( value == null )
            {
                return defaultValue;
            }
            else
            {
                return value;
            }
        };
    }

    public static <T> Setting<T> legacyFallback( Setting<T> fallbackSetting, Setting<T> newSetting )
    {
        return new Setting<T>()
        {
            @Override
            public String name()
            {
                return newSetting.name();
            }

            public String getDefaultValue()
            {
                return newSetting.getDefaultValue();
            }

            public T from( Configuration config )
            {
                return newSetting.from( config );
            }

            public T apply( Function<String, String> config )
            {
                String newValue = config.apply( newSetting.name() );
                return newValue == null ? fallbackSetting.apply( config ) : newSetting.apply( config );
            }

            @Override
            public void withScope( Function<String,String> scopingRule )
            {
                newSetting.withScope( scopingRule );
            }

            @Override
            public String valueDescription()
            {
                return newSetting.valueDescription();
            }

            @Override
            public Optional<String> description()
            {
                return newSetting.description();
            }

            @Override
            public boolean dynamic()
            {
                return newSetting.dynamic();
            }

            @Override
            public boolean deprecated()
            {
                return newSetting.deprecated();
            }

            @Override
            public Optional<String> replacement()
            {
                return newSetting.replacement();
            }

            @Override
            public boolean internal()
            {
                return newSetting.internal();
            }

            @Override
            public Optional<String> documentedDefaultValue()
            {
                return newSetting.documentedDefaultValue();
            }
        };
    }

    private Settings()
    {
        throw new AssertionError();
    }

    public static class DefaultSetting<T> extends ScopeAwareSetting<T> implements SettingHelper<T>
    {
        private final String name;
        private final Function<String, T> parser;
        private final BiFunction<String,Function<String,String>,String> valueLookup;
        private final BiFunction<String,Function<String,String>,String> defaultLookup;
        private final List<BiFunction<T,Function<String,String>,T>> valueConverters;

        protected DefaultSetting( String name, Function<String,T> parser,
                BiFunction<String,Function<String,String>,String> valueLookup,
                BiFunction<String,Function<String,String>,String> defaultLookup,
                List<BiFunction<T,Function<String,String>,T>> valueConverters )
        {
            this.name = name;
            this.parser = parser;
            this.valueLookup = valueLookup;
            this.defaultLookup = defaultLookup;
            this.valueConverters = valueConverters;
        }

        @Override
        protected String provideName()
        {
            return name;
        }

        @Override
        public String getDefaultValue()
        {
            return defaultLookup( from -> null );
        }

        @Override
        public T from( Configuration config )
        {
            return config.get( this );
        }

        @Override
        public Optional<Function<String, T>> getParser()
        {
            return Optional.of( parser );
        }

        @Override
        public String lookup( Function<String, String> settings )
        {
            return valueLookup.apply( name(), settings );
        }

        @Override
        public String defaultLookup( Function<String, String> settings )
        {
            return defaultLookup.apply( name(), settings );
        }

        @Override
        public T apply( Function<String, String> settings )
        {
            // Lookup value as string
            String value = lookup( settings );

            // Try defaults
            if ( value == null )
            {
                try
                {
                    value = defaultLookup( settings );
                }
                catch ( Exception e )
                {
                    throw new IllegalArgumentException( String.format( "Missing mandatory setting '%s'", name() ) );
                }
            }

            // If still null, return null
            if ( value == null )
            {
                return null;
            }

            // Parse value
            T result;
            try
            {
                result = parser.apply( value );
                // Apply converters and constraints
                if ( valueConverters != null )
                {
                    for ( BiFunction<T,Function<String,String>,T> valueConverter : valueConverters )
                    {
                        result = valueConverter.apply( result, settings );
                    }
                }
            }
            catch ( IllegalArgumentException e )
            {
                throw new InvalidSettingException( name(), value, e.getMessage() );
            }

            return result;
        }

        @Override
        public String valueDescription()
        {
            StringBuilder builder = new StringBuilder(  );
            builder.append( name() ).append( " is " ).append( parser.toString() );

            if ( valueConverters != null && valueConverters.size() > 0 )
            {
                builder.append( " which " );
                boolean first = true;
                for ( BiFunction<T,Function<String,String>,T> valueConverter : valueConverters )
                {
                    if ( !first )
                    {
                        builder.append( ", and " );
                    }
                    builder.append( valueConverter );
                    first = false;
                }
            }

            return builder.toString();
        }
    }
}
