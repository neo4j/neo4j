/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 */
public class Args
{
    private final String[] args;
    private final Map<String, String> map = new HashMap<String, String>();
    private final List<String> orphans = new ArrayList<String>();
//    private final Set<String> touchedParameters = new HashSet<String>();
//    private final BitSet touchedOrphans = new BitSet();

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
        putAll( source );
    }

    public String[] source()
    {
        return this.args;
    }

    public Map<String, String> asMap()
    {
        return new HashMap<String, String>( this.map );
    }

    public boolean has( String  key )
    {
        return this.map.containsKey( key );
    }

    public String get( String key, String defaultValue )
    {
        String value = this.map.get( key );
        return value != null ? value : defaultValue;
    }

    public String get( String key, String defaultValueIfNotFound, String defaultValueIfNoValue )
    {
        String value = this.map.get( key );
        if ( value != null )
        {
            return value;
        }
        return this.map.containsKey( key ) ? defaultValueIfNoValue : defaultValueIfNotFound;
    }

    public Number getNumber( String key, Number defaultValue )
    {
        String value = this.map.get( key );
        return value != null ? Double.parseDouble( value ) : defaultValue;
    }

    public Boolean getBoolean( String key, Boolean defaultValue )
    {
        String value = this.map.get( key );
        return value != null ? Boolean.parseBoolean( value ) : defaultValue;
    }

    public Boolean getBoolean( String key, Boolean defaultValueIfNotFound,
            Boolean defaultValueIfNoValue )
    {
        String value = this.map.get( key );
        if ( value != null )
        {
            return Boolean.parseBoolean( value );
        }
        return this.map.containsKey( key ) ? defaultValueIfNoValue : defaultValueIfNotFound;
    }
    
    public <T extends Enum<T>> T getEnum( Class<T> enumClass, String key, T defaultValue )
    {
        String raw = this.map.get( key );
        if ( raw == null )
            return defaultValue;
        
        for ( T candidate : enumClass.getEnumConstants() )
            if ( candidate.name().equals( raw ) )
                return candidate;
        throw new IllegalArgumentException( "No enum instance '" + raw + "' in " + enumClass.getName() );
    }

    public Object put( String key, String value )
    {
        return map.put( key, value );
    }

    public void putAll( Map<String, String> source )
    {
        this.map.putAll( source );
    }

    public List<String> orphans()
    {
        return new ArrayList<String>( this.orphans );
    }
    
    public String[] asArgs()
    {
        List<String> list = new ArrayList<String>();
        for ( String orphan : orphans )
        {
            String quote = orphan.contains( " " ) ? " " : "";
            list.add( quote + orphan + quote );
        }
        for ( Map.Entry<String, String> entry : map.entrySet() )
        {
            String quote = entry.getKey().contains( " " ) || entry.getValue().contains( " " ) ? " " : "";
            list.add( quote + (entry.getKey().length() > 1 ? "--" : "-") + entry.getKey() + "=" + entry.getValue() + quote );
        }
        return list.toArray( new String[0] );
    }
    
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for ( String arg : asArgs() )
            builder.append( builder.length() > 0 ? " " : "" ).append( arg );
        return builder.toString();
    }

    private static boolean isOption( String arg )
    {
        return arg.startsWith( "-" ) && arg.length() > 1;
    }

    private static String stripOption( String arg )
    {
        while ( arg.length() > 0 && arg.charAt( 0 ) == '-' )
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
                    if ( value.length() > 0 )
                    {
                        map.put( key, value );
                    }
                }
                else
                {
                    String key = arg;
                    int nextIndex = i+1;
                    String value = nextIndex < args.length ?
                        args[ nextIndex ] : null;
                    value = ( value == null || isOption( value ) ) ? null : value;
                    if ( value != null )
                    {
                        i = nextIndex;
                    }
                    map.put( key, value );
                }
            }
            else
            {
                orphans.add( arg );
            }
        }
    }

/*    public void printUntouched( PrintStream out )
    {
        boolean first = true;
        for ( Map.Entry<String, String> parameter : map.entrySet() )
        {
            if ( touchedParameters.contains( parameter ) ) continue;
            if ( first )
            {
                first = false;
                out.println( "Untouched parameters:" );
            }
            out.println( "  " + parameter.getKey() + ":" + parameter.getValue() );
        }

        first = true;
        for ( int i = 0; i < orphans.size(); i++ )
        {
            if ( touchedOrphans.get( i ) ) continue;
            if ( first )
            {
                first = false;
                out.println( "Untouched orphans:" );
            }
            out.println( "(" + i + "):" + orphans.get( i ) );
        }
    }

    public Map<String, String> getUntouchedParameters()
    {
        return null;
    }

    public Iterable<String> getUntouchedOrphans()
    {
        return null;
    }*/

    public static String jarUsage( Class<?> main, String... params )
    {
        StringBuilder usage = new StringBuilder( "USAGE: java [-cp ...] " );
        try
        {
            String jar = main.getProtectionDomain().getCodeSource().getLocation().getPath();
            usage.append( "-jar " ).append( jar );
        }
        catch ( Exception ex )
        {
            // ignore
        }
        usage.append( ' ' ).append( main.getCanonicalName() );
        for ( String param : params )
        {
            usage.append( ' ' ).append( param );
        }
        return usage.toString();
    }
}
