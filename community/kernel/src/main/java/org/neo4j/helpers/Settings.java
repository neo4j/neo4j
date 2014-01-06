/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.FileUtils;

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
public final class Settings
{
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

    public static final String TRUE = "true";
    public static final String FALSE = "false";

    public static final String DURATION_FORMAT = "\\d+(ms|s|m)";
    public static final String SIZE_FORMAT = "\\d+[kmgKMG]?";

    public static final String ANY = ".+";

    @SuppressWarnings("unchecked")
    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final String defaultValue )
    {
        return setting( name, parser, defaultValue, (Setting<T>) null );
    }

    public static <T> Setting<T> setting( final String name, final Function<String, T> parser,
                                          final String defaultValue,
                                          final Function2<T, Function<String, String>, T>... valueConverters )
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
                                          final Setting<T> inheritedSetting, final Function2<T, Function<String,
            String>, T>... valueConverters )
    {
        Function<Function<String, String>, String> valueLookup = named( name );

        Function<Function<String, String>, String> defaultLookup;
        if ( defaultValue != null )
        {
            //noinspection StringEquality
            if ( defaultValue.equals( MANDATORY ) )
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
            return "a duration";
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

                return Long.parseLong( mem.trim() ) * multiplier;
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
            return "a byte size";
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
     * For values such as:
     * <ul>
     *   <li>100M</li>   ==> 100 * 1024 * 1024
     *   <li>37261</li>  ==> 37261
     *   <li>2g</li>     ==> 2 * 1024 * 1024 * 1024
     *   <li>50m</li>    ==> 50 * 1024 * 1024
     *   <li>10k</li>    ==> 10 * 1024
     * </ul>
     */
    public static final Function<String, Long> LONG_WITH_OPTIONAL_UNIT = new Function<String, Long>()
    {
        @Override
        public Long apply( String from )
        {
            return Config.parseLongWithUnit( from );
        }
    };

    public static <T extends Enum> Function<String, T> options( final Class<T> enumClass )
    {
        return options( EnumSet.allOf( enumClass ) );
    }

    public static <T> Function<String, T> options( T... optionValues )
    {
        return Settings.<T>options( Iterables.<T,T>iterable( optionValues ) );
    }

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
                builder.append( "one of " );
                String comma = "";
                for ( T optionValue : optionValues )
                {
                    builder.append( comma ).append( optionValue.toString() );
                    comma = ", ";
                }
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
                String[] parts = value.split( separator );
                List<T> list = new ArrayList<T>();
                for ( String part : parts )
                {
                    list.add( itemParser.apply( part ) );
                }
                return list;
            }

            @Override
            public String toString()
            {
                return "a list separated by '"+separator+"' where items are "+itemParser.toString();
            }
        };
    }

    // Modifiers
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
        };
    }

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
        };
    }

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
        };
    }

    public static <T extends Comparable<T>> Function2<T, Function<String, String>, T> range( final T min, final T max )
    {
        return Functions.<T, Function<String, String>>compose2().apply( min( min ), max( max ) );
    }

    public static final Function2<Integer, Function<String, String>, Integer> port =
            illegalValueMessage( "must be a valid port number(1-65535)", range( 1, 65535 ) );

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
        };
    }

    public static Function2<String, Function<String, String>, String> toLowerCase =
            new Function2<String, Function<String, String>, String>()
            {
                @Override
                public String apply( String value, Function<String, String> settings )
                {
                    return value.toLowerCase();
                }
            };

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
                return "relative to '"+baseSetting.name()+"'";
            }
        };
    }

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

    public static boolean osIsWindows()
    {
        String nameOs = System.getProperty( "os.name" );
        return nameOs.startsWith( "Windows" );
    }

    public static boolean osIsMacOS()
    {
        String nameOs = System.getProperty( "os.name" );
        return nameOs.equalsIgnoreCase( "Mac OS X" );
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
                throw new IllegalArgumentException( String.format( "Bad value '%s' for setting '%s': %s", value, name(), e.getMessage() ) );
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
