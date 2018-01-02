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
package org.neo4j.helpers;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;

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
 * @deprecated this class is deprecated and will be moved to internal packages in the next major release
 */
@Deprecated
public final class Settings
{
    // NOTE: Do not use this class, use org.neo4j.kernel.configuration.Settings instead

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
    @Deprecated public static final String MANDATORY = org.neo4j.kernel.configuration.Settings.MANDATORY;
    @Deprecated public static final String NO_DEFAULT = null;
    @Deprecated public static final String EMPTY = "";

    @Deprecated public static final String TRUE = "true";
    @Deprecated public static final String FALSE = "false";

    @Deprecated public static final String DEFAULT = "default";

    @Deprecated public static final String SEPARATOR = ",";

    @Deprecated public static final String DURATION_FORMAT = "\\d+(ms|s|m)";
    @Deprecated public static final String SIZE_FORMAT = "\\d+[kmgKMG]?";

    private static final String DURATION_UNITS = DURATION_FORMAT.substring(
            DURATION_FORMAT.indexOf( '(' ) + 1, DURATION_FORMAT.indexOf( ')' ) )
            .replace( "|", ", " );
    private static final String SIZE_UNITS = Arrays.toString(
            SIZE_FORMAT.substring( SIZE_FORMAT.indexOf( '[' ) + 1,
                    SIZE_FORMAT.indexOf( ']' ) )
                    .toCharArray() )
            .replace( "[", "" )
            .replace( "]", "" );

    @Deprecated public static final String ANY = ".+";

    @Deprecated
    @SuppressWarnings("unchecked")
    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final String defaultValue )
    {
        return setting( name, parser, defaultValue, (Setting<T>) null );
    }

    @Deprecated
    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final String defaultValue,
                                          final Function2<T, Function<String, String>, T>... valueConverters )
    {
        return setting( name, parser, defaultValue, null, valueConverters );
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final Setting<T> inheritedSetting )
    {
        return setting( name, parser, null, inheritedSetting );
    }

    @Deprecated
    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final String defaultValue,
                                          final Setting<T> inheritedSetting, final Function2<T, Function<String,
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
            defaultLookup = Functions.nullFunction();
        }

        if ( inheritedSetting != null )
        {
            valueLookup = inheritedValue( valueLookup, inheritedSetting );
            defaultLookup = inheritedDefault( defaultLookup, inheritedSetting );
        }

        return new DefaultSetting<T>( name, parser, valueLookup, defaultLookup, valueConverters );
    }

    private static <T> Function<Function<String, String>, String> inheritedValue( final Function<Function<String,
            String>, String> lookup, final Setting<T> inheritedSetting )
    {
        return new Function<Function<String, String>, String>()
        {
            @Override
            public String apply( Function<String, String> settings )
            {
                String value = lookup.apply( settings );
                if ( value == null )
                {
                    value = ((SettingHelper<T>) inheritedSetting).lookup( settings );
                }
                return value;
            }
        };
    }

    private static <T> Function<Function<String, String>, String> inheritedDefault( final Function<Function<String,
            String>, String> lookup, final Setting<T> inheritedSetting )
    {
        return new Function<Function<String, String>, String>()
        {
            @Override
            public String apply( Function<String, String> settings )
            {
                String value = lookup.apply( settings );
                if ( value == null )
                {
                    value = ((SettingHelper<T>) inheritedSetting).defaultLookup( settings );
                }
                return value;
            }
        };
    }

    @Deprecated
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

    @Deprecated
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

    @Deprecated
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

    @Deprecated
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

    @Deprecated
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

    @Deprecated
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

    @Deprecated
    public static final Function<String, List<String>> STRING_LIST = new Function<String, List<String>>()
    {
        @Override
        public List<String> apply( String value )
        {
            String[] list = value.split( SEPARATOR );
            List<String> result = new ArrayList();
            for( String item : list)
            {
                item = item.trim();
                if( !item.equals( "" ) )
                    result.add( item );
            }
            return result;
        }

        @Override
        public String toString()
        {
            return "a comma-seperated string";
        }
    };

    @Deprecated
    public static final Function<String, HostnamePort> HOSTNAME_PORT = new Function<String, HostnamePort>()
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

    @Deprecated
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
            return "a duration (valid units are `" + DURATION_UNITS.replace( ", ", "`, `" ) + "`)";
        }
    };

    @Deprecated
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

    @Deprecated
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

    @Deprecated
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

    @Deprecated
    public static final Function<String, File> PATH = new Function<String, File>()
    {
        @Override
        public File apply( String setting )
        {
            setting = FileUtils.fixSeparatorsInPath( setting );

            return new File( setting );
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
    @Deprecated
    public static final Function<String, Long> LONG_WITH_OPTIONAL_UNIT = new Function<String, Long>()
    {
        @Override
        public Long apply( String from )
        {
            return Config.parseLongWithUnit( from );
        }
    };

    @Deprecated
    public static <T extends Enum> Function<String, T> options( final Class<T> enumClass )
    {
        return options( EnumSet.allOf( enumClass ) );
    }

    @Deprecated
    public static <T> Function<String, T> options( T... optionValues )
    {
        return Settings.<T>options( Iterables.<T,T>iterable( optionValues ) );
    }

    @Deprecated
    public static <T> Function<String, T> options( final Iterable<T> optionValues )
    {
        return new Function<String, T>()
        {
            @Override
            public T apply( String value )
            {
                for ( T optionValue : optionValues )
                {
                    if ( optionValue.toString().equals( value ) )
                    {
                        return optionValue;
                    }
                }
                throw new IllegalArgumentException( "must be one of " + Iterables.toList( optionValues ).toString() );
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

    @Deprecated
    public static <T> Function<String, List<T>> list( final String separator, final Function<String, T> itemParser )
    {
        return new Function<String, List<T>>()
        {
            @Override
            public List<T> apply( String value )
            {
                List<T> list = new ArrayList<T>();
                if ( value.length() > 0 )
                {
                    String[] parts = value.split( separator );
                    for ( String part : parts )
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
    @Deprecated
    public static Function2<String, Function<String, String>, String> matches( final String regex )
    {
        final Pattern pattern = Pattern.compile( regex );

        return new Function2<String, Function<String, String>, String>()
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

    @Deprecated
    public static <T extends Comparable<T>> Function2<T, Function<String, String>, T> min( final T min )
    {
        return new Function2<T, Function<String, String>, T>()
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

    @Deprecated
    public static <T extends Comparable<T>> Function2<T, Function<String, String>, T> max( final T max )
    {
        return new Function2<T, Function<String, String>, T>()
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

    @Deprecated
    public static <T extends Comparable<T>> Function2<T, Function<String, String>, T> range( final T min, final T max )
    {
        return new Function2<T, Function<String, String>, T>()
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

    @Deprecated
    public static final Function2<Integer, Function<String, String>, Integer> port =
            illegalValueMessage( "must be a valid port number", range( 0, 65535 ) );

    @Deprecated
    public static <T> Function2<T, Function<String, String>, T> illegalValueMessage( final String message,
                                                                                     final Function2<T,
                                                                                             Function<String,
                                                                                                     String>,
                                                                                             T> valueFunction )
    {
        return new Function2<T, Function<String, String>, T>()
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

    @Deprecated
    public static Function2<String, Function<String, String>, String> toLowerCase =
            new Function2<String, Function<String, String>, String>()
            {
                @Override
                public String apply( String value, Function<String, String> settings )
                {
                    return value.toLowerCase();
                }
            };

    @Deprecated
    public static Function2<URI, Function<String, String>, URI> normalize =
            new Function2<URI, Function<String, String>, URI>()
            {
                @Override
                public URI apply( URI value, Function<String, String> settings )
                {
                    String resultStr = value.normalize().toString();
                    if ( resultStr.endsWith( "/" ) )
                    {
                        value = java.net.URI.create( resultStr.substring( 0, resultStr.length() - 1 ) );
                    }
                    return value;
                }
            };

    // Setting converters and constraints
    @Deprecated
    public static Function2<File, Function<String, String>, File> basePath( final Setting<File> baseSetting )
    {
        return new Function2<File, Function<String, String>, File>()
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

    @Deprecated
    public static Function2<File, Function<String, String>, File> isFile =
            new Function2<File, Function<String, String>, File>()
            {
                @Override
                public File apply( File path, Function<String, String> settings )
                {
                    if ( path.exists() && !path.isFile() )
                    {
                        throw new IllegalArgumentException( String.format( "%s must point to a file, not a directory",
                                path.toString() ) );
                    }

                    return path;
                }
            };

    @Deprecated
    public static Function2<File, Function<String, String>, File> isDirectory =
            new Function2<File, Function<String, String>, File>()
            {
                @Override
                public File apply( File path, Function<String, String> settings )
                {
                    if ( path.exists() && !path.isDirectory() )
                    {
                        throw new IllegalArgumentException( String.format( "%s must point to a file, not a directory",
                                path.toString() ) );
                    }

                    return path;
                }
            };

    // Setting helpers
    @Deprecated
    private static Function<Function<String, String>, String> named( final String name )
    {
        return new Function<Function<String, String>, String>()
        {
            @Override
            public String apply( Function<String, String> settings )
            {
                return settings.apply( name );
            }
        };
    }

    private static Function<Function<String, String>, String> withDefault( final String defaultValue,
                                                                           final Function<Function<String, String>,
                                                                                   String> lookup )
    {
        return new Function<Function<String, String>, String>()
        {
            @Override
            public String apply( Function<String, String> settings )
            {
                String value = lookup.apply( settings );
                if ( value == null )
                {
                    return defaultValue;
                }
                else
                {
                    return value;
                }
            }
        };
    }

    private static Function<Function<String, String>, String> mandatory( final Function<Function<String, String>,
            String> lookup )
    {
        return new Function<Function<String, String>, String>()
        {
            @Override
            public String apply( Function<String, String> settings )
            {
                String value = lookup.apply( settings );
                if ( value == null )
                {
                    throw new IllegalArgumentException( "mandatory setting is missing" );
                }
                return value;
            }
        };
    }

    private Settings()
    {
    }

    @Deprecated
    public static class DefaultSetting<T> implements SettingHelper<T>
    {
        private final String name;
        private final Function<String, T> parser;
        private final Function<Function<String, String>, String> valueLookup;
        private final Function<Function<String, String>, String> defaultLookup;
        private Function2<T, Function<String, String>, T>[] valueConverters;

        public DefaultSetting( String name, Function<String, T> parser,
                               Function<Function<String, String>, String> valueLookup, Function<Function<String,
                String>, String> defaultLookup,
                               Function2<T, Function<String, String>, T>... valueConverters )
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
            return defaultLookup( Functions.<String, String>nullFunction() );
        }

        public String lookup( Function<String, String> settings )
        {
            return valueLookup.apply( settings );
        }

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
                for ( Function2<T, Function<String, String>, T> valueConverter : valueConverters )
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
                    Function2<T, Function<String, String>, T> valueConverter = valueConverters[i];
                    if (i > 0)
                        builder.append( ", and " );
                    builder.append( valueConverter );
                }
            }


            return builder.toString();
        }
    }
}