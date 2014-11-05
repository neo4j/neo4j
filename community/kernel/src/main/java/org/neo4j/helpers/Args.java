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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.function.Function;
import org.neo4j.kernel.impl.util.Validator;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.IteratorUtil.single;

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
 */
public class Args
{
    private final String[] args;
    private final Map<String, List<String>> map = new HashMap<>();
    private final List<String> orphans = new ArrayList<>();

    /**
     * Suitable for main( String[] args )
     * @param args the arguments to parse.
     */
    public Args( String... args )
    {
        this.args = args;
        parseArgs( args );
    }

    public Args( Map<String, String> source )
    {
        this.args = null;
        for ( Entry<String,String> entry : source.entrySet() )
        {
            map.put( entry.getKey(), new ArrayList<>( Arrays.asList( entry.getValue() ) ) );
        }
    }

    public String[] source()
    {
        return this.args;
    }

    public Map<String, String> asMap()
    {
        Map<String,String> result = new HashMap<>();
        for ( Map.Entry<String,List<String>> entry : map.entrySet() )
        {
            result.put( entry.getKey(), first( entry.getValue() ) );
        }
        return result;
    }

    public boolean has( String  key )
    {
        return this.map.containsKey( key );
    }

    public boolean hasNonNull( String key )
    {
        List<String> values = this.map.get( key );
        return values != null && !values.isEmpty();
    }

    private String getSingleOptionOrNull( String key )
    {
        List<String> values = this.map.get( key );
        if ( values == null || values.isEmpty() )
        {
            return null;
        }
        else if ( values.size() > 1 )
        {
            throw new IllegalArgumentException( "There are multiple values for '" + key + "'" );
        }
        return single( values );
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

    public Boolean getBoolean( String key, Boolean defaultValue )
    {
        String value = getSingleOptionOrNull( key );
        return value != null ? Boolean.parseBoolean( value ) : defaultValue;
    }

    public Boolean getBoolean( String key, Boolean defaultValueIfNotFound,
            Boolean defaultValueIfNoValue )
    {
        String value = getSingleOptionOrNull( key );
        if ( value != null )
        {
            return Boolean.parseBoolean( value );
        }
        return this.map.containsKey( key ) ? defaultValueIfNoValue : defaultValueIfNotFound;
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

    public String[] asArgs()
    {
        List<String> list = new ArrayList<>();
        for ( String orphan : orphans )
        {
            String quote = orphan.contains( " " ) ? " " : "";
            list.add( quote + orphan + quote );
        }
        for ( Map.Entry<String,List<String>> entry : map.entrySet() )
        {
            String key = entry.getKey();
            List<String> values = entry.getValue();

            for ( String value : values )
            {
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

    private static String stripOption( String arg )
    {
        while ( !arg.isEmpty() && arg.charAt( 0 ) == '-' )
        {
            arg = arg.substring( 1 );
        }
        return arg;
    }

    private void parseArgs( String[] args )
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
                        put( key, value );
                    }
                }
                else
                {
                    int nextIndex = i+1;
                    String value = nextIndex < args.length ?
                        args[ nextIndex ] : null;
                    value = ( value == null || isOption( value ) ) ? null : value;
                    if ( value != null )
                    {
                        i = nextIndex;
                    }
                    put( arg, value );
                }
            }
            else
            {
                orphans.add( arg );
            }
        }
    }

    private void put( String key, String value )
    {
        List<String> values = map.get( key );
        if ( values == null )
        {
            map.put( key, values = new ArrayList<>() );
        }
        values.add( value );
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
     * An option can be specified multiple times. This method will allow interpreting all values for
     * the given key, returning a {@link Collection}. This is the only means of extracting multiple values
     * for any given option. All other methods revolve around zero or one value for an option.
     */
    @SafeVarargs
    public final <T> Collection<T> interpretOptions( String key, Function<String,T> defaultValue,
            Function<String,T> converter, Validator<T>... validators )
    {
        Collection<T> values = new ArrayList<>();
        if ( !hasNonNull( key ) )
        {
            values.add( defaultValue.apply( key ) );
        }
        else
        {
            for ( String stringValue : map.get( key ) )
            {
                values.add( converter.apply( stringValue ) );
            }
        }

        for ( T value : values )
        {
            validated( value, validators );
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
