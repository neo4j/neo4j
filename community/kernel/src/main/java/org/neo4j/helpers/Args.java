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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.neo4j.function.Function;
import org.neo4j.kernel.impl.util.Validator;

import static org.neo4j.helpers.collection.IteratorUtil.firstOrNull;

/**
 * Parses a String[] argument from a main-method. It expects values to be either
 * key/value pairs or just "orphan" values (w/o a key associated).
 * <p>
 * A key is defined with one or more dashes in the beginning, for example:
 *
 * <pre>
 *   '-path'
 *   '--path'
 * </pre>
 *
 * A key/value pair can be either one single String from the array where there's
 * a '=' delimiter between the key and value, like so:
 *
 * <pre>
 *   '--path=/my/path/to/something'
 * </pre>
 * ...or consist of two (consecutive) strings from the array, like so:
 * <pre>
 *   '-path' '/my/path/to/something'
 * </pre>
 *
 * Multiple values for an option is supported, however the only means of extracting all values is be
 * using {@link #interpretOptions(String, Function, Function, Validator...)}, all other methods revolve
 * around single value, i.e. will fail if there are multiple.
 *
 * Options can have metadata which can be extracted using
 * {@link #interpretOptions(String, Function, Function, Validator...)}. Metadata looks like:
 * <pre>
 *   --my-option:Metadata my-value
 * </pre>
 *
 * where {@code Metadata} would be the metadata of {@code my-value}.
 */
public class Args
{
    private static final char OPTION_METADATA_DELIMITER = ':';

    public static class ArgsParser
    {
        private final String[] flags;

        private ArgsParser( String... flags )
        {
            this.flags = Objects.requireNonNull( flags );
        }

        public Args parse( String... arguments )
        {
            return new Args( flags, arguments );
        }
    }

    public static class Option<T>
    {
        private final T value;
        private final String metadata;

        private Option( T value, String metadata )
        {
            this.value = value;
            this.metadata = metadata;
        }

        public T value()
        {
            return value;
        }

        public String metadata()
        {
            return metadata;
        }

        @Override
        public String toString()
        {
            return "Option[" + value + (metadata != null ? ", " + metadata : "") + "]";
        }
    }

    private static final Function<String,Option<String>> DEFAULT_OPTION_PARSER = new Function<String,Option<String>>()
    {
        @Override
        public Option<String> apply( String from )
        {
            int metadataStartIndex = from.indexOf( OPTION_METADATA_DELIMITER );
            return metadataStartIndex == -1
                    ? new Option<>( from, null )
                    : new Option<>( from.substring( 0, metadataStartIndex ), from.substring( metadataStartIndex + 1 ) );
        }
    };

    private final String[] args;
    private final String[] flags;
    private final Map<String, List<Option<String>>> map = new HashMap<>();
    private final List<String> orphans = new ArrayList<>();

    public static ArgsParser withFlags( String... flags )
    {
        return new ArgsParser( flags );
    }

    public static Args parse( String...args )
    {
        return withFlags().parse( args );
    }

    /**
     * Suitable for main( String[] args )
     * @param args the arguments to parse.
     */
    private Args( String[] flags, String[] args )
    {
        this( DEFAULT_OPTION_PARSER, flags, args );
    }

    /**
     * Suitable for main( String[] args )
     * @param flags list of possible flags (e.g -v or -skip-bad). A flag differs from an option in that it
     * has no value after it. This list of flags is used for distinguishing between the two.
     * @param args the arguments to parse.
     */
    private Args( Function<String,Option<String>> optionParser, String[] flags, String[] args )
    {
        this.flags = flags;
        this.args = args;
        parseArgs( optionParser, args );
    }

    public Args( Map<String, String> source )
    {
        this( DEFAULT_OPTION_PARSER, source );
    }

    public Args( Function<String,Option<String>> optionParser, Map<String, String> source )
    {
        this.flags = new String[] {};
        this.args = null;
        for ( Entry<String,String> entry : source.entrySet() )
        {
            put( optionParser, entry.getKey(), entry.getValue() );
        }
    }

    public String[] source()
    {
        return this.args;
    }

    public Map<String, String> asMap()
    {
        Map<String,String> result = new HashMap<>();
        for ( Map.Entry<String,List<Option<String>>> entry : map.entrySet() )
        {
            Option<String> value = firstOrNull( entry.getValue() );
            result.put( entry.getKey(), value != null ? value.value() : null );
        }
        return result;
    }

    public boolean has( String  key )
    {
        return this.map.containsKey( key );
    }

    public boolean hasNonNull( String key )
    {
        List<Option<String>> values = this.map.get( key );
        return values != null && !values.isEmpty();
    }

    private String getSingleOptionOrNull( String key )
    {
        List<Option<String>> values = this.map.get( key );
        if ( values == null || values.isEmpty() )
        {
            return null;
        }
        else if ( values.size() > 1 )
        {
            throw new IllegalArgumentException( "There are multiple values for '" + key + "'" );
        }
        return values.get( 0 ).value();
    }

    public String get( String key )
    {
        return getSingleOptionOrNull( key );
    }

    public String get( String key, String defaultValue )
    {
        String value = getSingleOptionOrNull( key );
        return value != null ? value : defaultValue;
    }

    public String get( String key, String defaultValueIfNotFound, String defaultValueIfNoValue )
    {
        String value = getSingleOptionOrNull( key );
        if ( value != null )
        {
            return value;
        }
        return this.map.containsKey( key ) ? defaultValueIfNoValue : defaultValueIfNotFound;
    }

    public Number getNumber( String key, Number defaultValue )
    {
        String value = getSingleOptionOrNull( key );
        return value != null ? Double.parseDouble( value ) : defaultValue;
    }

    public long getDuration( String key, long defaultValueInMillis)
    {
        String value = getSingleOptionOrNull( key );
        return value != null ? TimeUtil.parseTimeMillis.apply(value) : defaultValueInMillis;
    }

    /**
     * Like calling {@link #getBoolean(String, Boolean)} with {@code false} for default value.
     * This is the most common case, i.e. returns {@code true} if boolean argument was specified as:
     * <ul>
     * <li>--myboolean</li>
     * <li>--myboolean=true</li>
     * </ul>
     * Otherwise {@code false.
     * }
     * @param key argument key.
     * @return {@code true} if argument was specified w/o value, or w/ value {@code true}, otherwise {@code false}.
     */
    public boolean getBoolean( String key )
    {
        return getBoolean( key, false );
    }

    /**
     * Like calling {@link #getBoolean(String, Boolean, Boolean)} with {@code true} for
     * {@code defaultValueIfNoValue}, i.e. specifying {@code --myarg} will interpret it as {@code true}.
     */
    public Boolean getBoolean( String key, Boolean defaultValueIfNotSpecified )
    {
        return getBoolean( key, defaultValueIfNotSpecified, Boolean.TRUE );
    }

    /**
     * Parses a {@code boolean} argument. There are a couple of cases:
     * <ul>
     * <li>The argument isn't specified. In this case the value of {@code defaultValueIfNotSpecified}
     * will be returned.</li>
     * <li>The argument is specified without value, for example <pre>--myboolean</pre>. In this case
     * the value of {@code defaultValueIfNotSpecified} will be returned.</li>
     * <li>The argument is specified with value, for example <pre>--myboolean=true</pre>.
     * In this case the actual value will be returned.</li>
     * </ul>
     *
     * @param key argument key.
     * @param defaultValueIfNotSpecified used if argument not specified.
     * @param defaultValueIfSpecifiedButNoValue used if argument specified w/o value.
     * @return argument boolean value depending on what was specified, see above.
     */
    public Boolean getBoolean( String key, Boolean defaultValueIfNotSpecified,
            Boolean defaultValueIfSpecifiedButNoValue )
    {
        String value = getSingleOptionOrNull( key );
        if ( value != null )
        {
            return Boolean.parseBoolean( value );
        }
        return this.map.containsKey( key ) ? defaultValueIfSpecifiedButNoValue : defaultValueIfNotSpecified;
    }

    public <T extends Enum<T>> T getEnum( Class<T> enumClass, String key, T defaultValue )
    {
        String raw = getSingleOptionOrNull( key );
        if ( raw == null )
        {
            return defaultValue;
        }

        for ( T candidate : enumClass.getEnumConstants() )
        {
            if ( candidate.name().equals( raw ) )
            {
                return candidate;
            }
        }
        throw new IllegalArgumentException( "No enum instance '" + raw + "' in " + enumClass.getName() );
    }

    public List<String> orphans()
    {
        return new ArrayList<>( this.orphans );
    }

    public String[] orphansAsArray()
    {
        return orphans.toArray( new String[orphans.size()] );
    }

    public String[] asArgs()
    {
        List<String> list = new ArrayList<>();
        for ( String orphan : orphans )
        {
            String quote = orphan.contains( " " ) ? " " : "";
            list.add( quote + orphan + quote );
        }
        for ( Map.Entry<String,List<Option<String>>> entry : map.entrySet() )
        {
            for ( Option<String> option : entry.getValue() )
            {
                String key = option.metadata != null
                        ? entry.getKey() + OPTION_METADATA_DELIMITER + option.metadata()
                        : entry.getKey();
                String value = option.value();
                String quote = key.contains( " " ) || (value != null && value.contains( " " )) ? " " : "";
                list.add( quote + (key.length() > 1 ? "--" : "-") + key + (value != null ? "=" + value + quote : "") );
            }
        }
        return list.toArray( new String[list.size()] );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for ( String arg : asArgs() )
        {
            builder.append( builder.length() > 0 ? " " : "" ).append( arg );
        }
        return builder.toString();
    }

    private static boolean isOption( String arg )
    {
        return arg.startsWith( "-" ) && arg.length() > 1;
    }

    private boolean isFlag( String arg )
    {
        return ArrayUtil.contains( flags, arg );
    }

    private static boolean isBoolean( String value )
    {
        return (value != null) && ("true".equalsIgnoreCase( value ) || "false".equalsIgnoreCase( value ));
    }

    private static String stripOption( String arg )
    {
        while ( !arg.isEmpty() && arg.charAt( 0 ) == '-' )
        {
            arg = arg.substring( 1 );
        }
        return arg;
    }

    private void parseArgs( Function<String,Option<String>> optionParser, String[] args )
    {
        for ( int i = 0; i < args.length; i++ )
        {
            String arg = args[ i ];
            if ( isOption( arg ) )
            {
                arg = stripOption( arg );
                int equalIndex = arg.indexOf( '=' );
                if ( equalIndex != -1 )
                {
                    String key = arg.substring( 0, equalIndex );
                    String value = arg.substring( equalIndex + 1 );
                    if ( !value.isEmpty() )
                    {
                        put( optionParser, key, value );
                    }
                }
                else if ( isFlag( arg ) )
                {
                    int nextIndex = i + 1;
                    String value = nextIndex < args.length ? args[nextIndex] : null;
                    if ( isBoolean( value ) )
                    {
                        i = nextIndex;
                        put( optionParser, arg, Boolean.valueOf( value ).toString() );
                    }
                    else
                    {
                        put( optionParser, arg, null );
                    }
                }
                else
                {
                    int nextIndex = i + 1;
                    String value = nextIndex < args.length ?
                        args[ nextIndex ] : null;
                    value = ( value == null || isOption( value ) ) ? null : value;
                    if ( value != null )
                    {
                        i = nextIndex;
                    }
                    put( optionParser, arg, value );
                }
            }
            else
            {
                orphans.add( arg );
            }
        }
    }

    private void put( Function<String,Option<String>> optionParser, String key, String value )
    {
        Option<String> option = optionParser.apply( key );
        List<Option<String>> values = map.get( option.value() );
        if ( values == null )
        {
            map.put( option.value(), values = new ArrayList<>() );
        }
        values.add( new Option<>( value, option.metadata() ) );
    }

    public static String jarUsage( Class<?> main, String... params )
    {
        StringBuilder usage = new StringBuilder( "USAGE: java [-cp ...] " );
        try
        {
            String jar = main.getProtectionDomain().getCodeSource().getLocation().getPath();
            usage.append( "-jar " ).append( jar );
        }
        catch ( Exception ignored )
        {
        }
        usage.append( ' ' ).append( main.getCanonicalName() );
        for ( String param : params )
        {
            usage.append( ' ' ).append( param );
        }
        return usage.toString();
    }

    /**
     * Useful for printing usage where the description itself shouldn't have knowledge about the width
     * of the window where it's printed. Existing new-line characters in the text are honored.
     *
     * @param description text to split, if needed.
     * @param maxLength line length to split at, actually closest previous white space.
     * @return description split into multiple lines if need be.
     */
    public static String[] splitLongLine( String description, int maxLength )
    {
        List<String> lines = new ArrayList<>();
        while ( description.length() > 0 )
        {
            String line = description.substring( 0, Math.min( maxLength, description.length() ) );
            int position = line.indexOf( "\n" );
            if ( position > -1 )
            {
                line = description.substring( 0, position );
                lines.add( line );
                description = description.substring( position );
                if ( description.length() > 0 )
                {
                    description = description.substring( 1 );
                }
            }
            else
            {
                position = description.length() > maxLength ?
                        findSpaceBefore( description, maxLength ) : description.length();
                line = description.substring( 0, position );
                lines.add( line );
                description = description.substring( position );
            }
        }
        return lines.toArray( new String[lines.size()] );
    }

    private static int findSpaceBefore( String description, int position )
    {
        while ( !Character.isWhitespace( description.charAt( position ) ) )
        {
            position--;
        }
        return position + 1;
    }

    @SafeVarargs
    public final <T> T interpretOption( String key, Function<String,T> defaultValue,
            Function<String,T> converter, Validator<T>... validators )
    {
        T value;
        if ( !has( key ) )
        {
            value = defaultValue.apply( key );
        }
        else
        {
            String stringValue = get( key );
            value = converter.apply( stringValue );
        }
        return validated( value, validators );
    }

    /**
     * An option can be specified multiple times; this method will allow interpreting all values for
     * the given key, returning a {@link Collection}. This is the only means of extracting multiple values
     * for any given option. All other methods revolve around zero or one value for an option.
     *
     * @param key Key of the option
     * @param defaultValue Default value value of the option
     * @param converter Converter to use
     * @param validators Validators to use
     * @param <T> The type of the option values
     * @return The option values
     */
    @SafeVarargs
    public final <T> Collection<T> interpretOptions( String key, Function<String,T> defaultValue,
            Function<String,T> converter, Validator<T>... validators )
    {
        Collection<Option<T>> options = interpretOptionsWithMetadata( key, defaultValue, converter, validators );
        Collection<T> values = new ArrayList<>( options.size() );
        for ( Option<T> option : options )
        {
            values.add( option.value() );
        }
        return values;
    }

    /**
     * An option can be specified multiple times; this method will allow interpreting all values for
     * the given key, returning a {@link Collection}. This is the only means of extracting multiple values
     * for any given option. All other methods revolve around zero or one value for an option.
     * This is also the only means of extracting metadata about a options. Metadata can be supplied as part
     * of the option key, like --my-option:Metadata "my value".
     *
     * @param key Key of the option
     * @param defaultValue Default value value of the option
     * @param converter Converter to use
     * @param validators Validators to use
     * @param <T> The type of the option values
     * @return The option values
     */
    @SafeVarargs
    public final <T> Collection<Option<T>> interpretOptionsWithMetadata( String key, Function<String,T> defaultValue,
            Function<String,T> converter, Validator<T>... validators )
    {
        Collection<Option<T>> values = new ArrayList<>();
        if ( !hasNonNull( key ) )
        {
            T defaultItem = defaultValue.apply( key );
            if ( defaultItem != null )
            {
                values.add( new Option<>( validated( defaultItem, validators ), null ) );
            }
        }
        else
        {
            for ( Option<String> option : map.get( key ) )
            {
                String stringValue = option.value();
                values.add( new Option<>( validated( converter.apply( stringValue ), validators ), option.metadata() ) );
            }
        }
        return values;
    }

    @SafeVarargs
    public final <T> T interpretOrphan( int index, Function<String,T> defaultValue,
            Function<String,T> converter, Validator<T>... validators )
    {
        assert index >= 0;

        T value;
        if ( index >= orphans.size() )
        {
            value = defaultValue.apply( "argument at index " + index );
        }
        else
        {
            String stringValue = orphans.get( index );
            value = converter.apply( stringValue );
        }
        return validated( value, validators );
    }

    @SafeVarargs
    private final <T> T validated( T value, Validator<T>... validators )
    {
        if ( value != null )
        {
            for ( Validator<T> validator : validators )
            {
                validator.validate( value );
            }
        }
        return value;
    }
}
