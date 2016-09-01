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
package org.neo4j.kernel.configuration;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.TimeUtil;
import org.neo4j.helpers.collection.Iterables;

import static java.lang.Character.isDigit;
import static org.neo4j.io.fs.FileUtils.fixSeparatorsInPath;

/**
 * Create settings for configurations in Neo4j. See {@link org.neo4j.graphdb.factory.GraphDatabaseSettings} for example.
 *
 * Each setting has a name, a parser that converts a string to the type of the setting, a default value,
 * an optional inherited setting, and optional value converters.
 *
 * A parser is a function that takes a string and converts to some type T. The parser may throw IllegalArgumentException
 * if it fails.
 *
 * The default value is the string representation of what you want as default. Special values are the constants NO_DEFAULT,
 * which means that you don't want any default value at all, and MANDATORY, which means that the user has to specify a value
 * for this setting. Not providing a mandatory value for a setting leads to an IllegalArgumentException.
 *
 * If a setting does not have a provided value, and no default, then
 */
public class Settings
{
    private static final String MATCHES_PATTERN_MESSAGE = "matches the pattern `%s`";

    private interface SettingHelper<T>
            extends Setting<T>
    {
        String lookup( Function<String, String> settings );

        String defaultLookup( Function<String, String> settings );
    }

    // Set default value to this if user HAS to set a value
    @SuppressWarnings("RedundantStringConstructorCall")
    // It's an explicitly allocated string so identity equality checks work
    public static final String MANDATORY = new String( "mandatory" );
    public static final String NO_DEFAULT = null;
    public static final String EMPTY = "";

    public static final String TRUE = "true";
    public static final String FALSE = "false";

    public static final String DEFAULT = "default";

    public static final String SEPARATOR = ",";

    public static final String DURATION_FORMAT = "\\d+(ms|s|m)";
    public static final String SIZE_FORMAT = "\\d+[kmgKMG]?";

    private static final String DURATION_UNITS = DURATION_FORMAT.substring(
            DURATION_FORMAT.indexOf( '(' ) + 1, DURATION_FORMAT.indexOf( ')' ) )
            .replace( "|", ", " );
    private static final String SIZE_UNITS = Arrays.toString(
            SIZE_FORMAT.substring( SIZE_FORMAT.indexOf( '[' ) + 1,
                    SIZE_FORMAT.indexOf( ']' ) )
                    .toCharArray() )
            .replace( "[", "" )
            .replace( "]", "" );

    public static final String ANY = ".+";

    @SuppressWarnings("unchecked")
    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final String defaultValue )
    {
        return setting( name, parser, defaultValue, (Setting<T>) null );
    }

    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final String defaultValue,
                                          final BiFunction<T, Function<String, String>, T>... valueConverters )
    {
        return setting( name, parser, defaultValue, null, valueConverters );
    }

    @SuppressWarnings("unchecked")
    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final Setting<T> inheritedSetting )
    {
        return setting( name, parser, null, inheritedSetting );
    }

    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final String defaultValue,
                                          final Setting<T> inheritedSetting, final BiFunction<T, Function<String,
            String>, T>... valueConverters )
    {
        Function<Function<String, String>, String> valueLookup = named( name );

        Function<Function<String, String>, String> defaultLookup;
        if ( defaultValue != null )
        {
            // This is explicitly an identity comparison. We are comparing against the known instance above,
            // using String.equals() here would mean that we could not have settings that have the string "mandatory"
            // as its default value.
            //noinspection StringEquality
            if ( defaultValue == MANDATORY ) // yes, this really is supposed to be ==
            {
                defaultLookup = mandatory( valueLookup );
            }
            else
            {
                defaultLookup = withDefault( defaultValue, valueLookup );
            }
        }
        else
        {
            defaultLookup = from -> null;
        }

        if ( inheritedSetting != null )
        {
            valueLookup = inheritedValue( valueLookup, inheritedSetting );
            defaultLookup = inheritedDefault( defaultLookup, inheritedSetting );
        }

        return new DefaultSetting<>( name, parser, valueLookup, defaultLookup, valueConverters );
    }

    public static <OUT, IN1, IN2> Setting<OUT> derivedSetting( String name,
                                                               Setting<IN1> in1, Setting<IN2> in2,
                                                               BiFunction<IN1, IN2, OUT> derivation,
                                                               Function<String, OUT> overrideConverter)
    {
        return new Setting<OUT>()
        {
            @Override
            public String name()
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
                String override = config.apply( name );
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
        };
    }

    public static <OUT, IN1> Setting<OUT> derivedSetting( String name,
                                                          Setting<IN1> in1,
                                                          Function<IN1, OUT> derivation,
                                                          Function<String, OUT> overrideConverter)
    {
        return new Setting<OUT>()
        {
            @Override
            public String name()
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
                String override = config.apply( name );
                if ( override != null )
                {
                    return overrideConverter.apply( override );
                }
                return derivation.apply( in1.apply( config ) );
            }
        };
    }

    public static Setting<File> pathSetting( String name, String defaultValue )
    {
        return new Setting<File>()
        {
            @Override
            public String name()
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
                String value = config.apply( name );
                if ( value == null )
                {
                    value = defaultValue;
                }
                if (value == null)
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
            public String toString()
            {
                return "A filesystem path; relative paths are resolved against the installation root, _<neo4j-home>_";
            }
        };
    }

    /**
     * A setting for specifying the hostname of this server that should be advertised to other servers.
     * When unspecified, the system will try to resolve the hostname of localhost.
     */
    public static Setting<String> hostnameSetting( String name )
    {
        return new Setting<String>()
        {
            @Override
            public String name()
            {
                return name;
            }

            @Override
            public String getDefaultValue()
            {
                return "localhost";
            }

            @Override
            public String from( Configuration config )
            {
                return config.get( this );
            }

            @Override
            public String apply( Function<String, String> config )
            {
                String value = config.apply( name );
                if ( value != null )
                {
                    return value;
                }
                try
                {
                    return InetAddress.getLocalHost().getHostAddress();
                }
                catch ( UnknownHostException e )
                {
                    return getDefaultValue();
                }
            }

            @Override
            public String toString()
            {
                return "a hostname or IP address";
            }
        };
    }

    private static <T> Function<Function<String, String>, String> inheritedValue( final Function<Function<String,
            String>, String> lookup, final Setting<T> inheritedSetting )
    {
        return settings -> {
            String value = lookup.apply( settings );
            if ( value == null )
            {
                value = ((SettingHelper<T>) inheritedSetting).lookup( settings );
            }
            return value;
        };
    }

    private static <T> Function<Function<String, String>, String> inheritedDefault( final Function<Function<String,
            String>, String> lookup, final Setting<T> inheritedSetting )
    {
        return settings -> {
            String value = lookup.apply( settings );
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
                return Integer.parseInt( value );
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
                return Long.parseLong( value );
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
                return Float.parseFloat( value );
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
                return Double.parseDouble( value );
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

    public static final Function<String, Long> DURATION = new Function<String, Long>()
    {
        @Override
        public Long apply( String value )
        {
            return TimeUtil.parseTimeMillis.apply( value );
        }

        @Override
        public String toString()
        {
            return "a duration (valid units are `" + DURATION_UNITS.replace( ", ", "`, `" ) + "`; default unit is `ms`)";
        }
    };

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

                long bytes = Long.parseLong( mem.trim() ) * multiplier;
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

    /**
     * For values expressed with a unit such as {@code 100M}.
     *
     * <ul>
     *   <li>100M<br>   ==&gt; 100 * 1024 * 1024</li>
     *   <li>37261<br>  ==&gt; 37261</li>
     *   <li>2g<br>     ==&gt; 2 * 1024 * 1024 * 1024</li>
     *   <li>50m<br>    ==&gt; 50 * 1024 * 1024</li>
     *   <li>10k<br>    ==&gt; 10 * 1024</li>
     * </ul>
     */
    public static final Function<String, Long> LONG_WITH_OPTIONAL_UNIT = Settings::parseLongWithUnit;

    public static <T extends Enum<T>> Function<String, T> options( final Class<T> enumClass )
    {
        return options( EnumSet.allOf( enumClass ), false );
    }

    public static <T> Function<String, T> options( T... optionValues )
    {
        return options( Iterables.<T,T>iterable( optionValues ), false );
    }

    public static <T> Function<String, T> optionsIgnoreCase( T... optionValues )
    {
        return options( Iterables.<T,T>iterable( optionValues ), true );
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
                StringBuilder builder = new StringBuilder(  );
                builder.append( "one of `" );
                String comma = "";
                for ( T optionValue : optionValues )
                {
                    builder.append( comma ).append( optionValue.toString() );
                    comma = "`, `";
                }
                builder.append( "`" );
                return builder.toString();
            }
        };
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

    public static BiFunction<String, Function<String, String>, String> toLowerCase =
            ( value, settings ) -> value.toLowerCase();

    public static BiFunction<URI, Function<String, String>, URI> normalize =
            ( value, settings ) -> {
                String resultStr = value.normalize().toString();
                if ( resultStr.endsWith( "/" ) )
                {
                    value = java.net.URI.create( resultStr.substring( 0, resultStr.length() - 1 ) );
                }
                return value;
            };

    // Setting converters and constraints
    public static BiFunction<File, Function<String, String>, File> basePath( final Setting<File> baseSetting )
    {
        return new BiFunction<File, Function<String, String>, File>()
        {
            @Override
            public File apply( File path, Function<String, String> settings )
            {
                File parent = baseSetting.apply( settings );

                return path.isAbsolute() ? path : new File( parent, path.getPath() );
            }

            @Override
            public String toString()
            {
                return "is relative to " + baseSetting.name();
            }
        };
    }

    public static BiFunction<File, Function<String, String>, File> isFile =
            ( path, settings ) -> {
                if ( path.exists() && !path.isFile() )
                {
                    throw new IllegalArgumentException( String.format( "%s must point to a file, not a directory",
                            path.toString() ) );
                }

                return path;
            };

    public static BiFunction<File, Function<String, String>, File> isDirectory =
            ( path, settings ) -> {
                if ( path.exists() && !path.isDirectory() )
                {
                    throw new IllegalArgumentException( String.format( "%s must point to a file, not a directory",
                            path.toString() ) );
                }

                return path;
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

        return Long.parseLong( number ) * multiplier;
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
    private static Function<Function<String, String>, String> named( final String name )
    {
        return settings -> settings.apply( name );
    }

    private static Function<Function<String, String>, String> withDefault( final String defaultValue,
                                                                           final Function<Function<String, String>,
                                                                                   String> lookup )
    {
        return settings -> {
            String value = lookup.apply( settings );
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

    private static Function<Function<String, String>, String> mandatory( final Function<Function<String, String>,
            String> lookup )
    {
        return settings -> {
            String value = lookup.apply( settings );
            if ( value == null )
            {
                throw new IllegalArgumentException( "mandatory setting is missing" );
            }
            return value;
        };
    }

    private Settings()
    {
    }

    public static class DefaultSetting<T> implements SettingHelper<T>
    {
        private final String name;
        private final Function<String, T> parser;
        private final Function<Function<String, String>, String> valueLookup;
        private final Function<Function<String, String>, String> defaultLookup;
        private final BiFunction<T, Function<String, String>, T>[] valueConverters;

        public DefaultSetting( String name, Function<String, T> parser,
                               Function<Function<String, String>, String> valueLookup, Function<Function<String,
                String>, String> defaultLookup, BiFunction<T, Function<String, String>, T>... valueConverters )
        {
            this.name = name;
            this.parser = parser;
            this.valueLookup = valueLookup;
            this.defaultLookup = defaultLookup;
            this.valueConverters = valueConverters;
        }

        @Override
        public String name()
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
        public String lookup( Function<String, String> settings )
        {
            return valueLookup.apply( settings );
        }

        @Override
        public String defaultLookup( Function<String, String> settings )
        {
            return defaultLookup.apply( settings );
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
                for ( BiFunction<T, Function<String, String>, T> valueConverter : valueConverters )
                {
                    result = valueConverter.apply( result, settings );
                }
            }
            catch ( IllegalArgumentException e )
            {
                throw new InvalidSettingException( name(), value, e.getMessage() );
            }

            return result;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder(  );
            builder.append( name ).append( " is " ).append( parser.toString() );

            if (valueConverters.length > 0)
            {
                builder.append( " which " );
                for ( int i = 0; i < valueConverters.length; i++ )
                {
                    BiFunction<T, Function<String, String>, T> valueConverter = valueConverters[i];
                    if (i > 0)
                        builder.append( ", and " );
                    builder.append( valueConverter );
                }
            }

            return builder.toString();
        }
    }
}
