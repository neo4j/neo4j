/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.neo4j.helpers.collection.ClosableIterator;

import static java.lang.Character.isDigit;

import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asIterator;

public class Launch4jIniValue implements Value<Integer>
{
    private static final String MAX_HEAP_SIZE_PREFIX = "-Xmx";
    
    private final File iniFile;
    
    public Launch4jIniValue( Environment environment )
    {
        File appFile = environment.getAppFile();
        this.iniFile = environment.isRunByApp() ?
                new File( appFile.getAbsoluteFile().getParentFile(), extensionLess( appFile.getName() ) + ".l4j.ini" ) : null;
    }

    private String extensionLess( String name )
    {
        int dot = name.lastIndexOf( '.' );
        return dot != -1 ? name.substring( 0, dot ) : name;
    }

    @Override
    public Integer get()
    {
        if ( iniFile != null && iniFile.exists() )
        {
            try
            {
                ClosableIterator<String> iterator = asIterator( iniFile );
                while ( iterator.hasNext() )
                {
                    String line = normalize( iterator.next() );
                    if ( isMaxHeapArgument( line ) )
                    {
                        String rawValue = parseHeapArgumentValue( line );
                        return parseJvmHeapSize( rawValue );
                    }
                }
            }
            catch ( IOException e )
            {
                // OK I'd guess
            }
        }
        return (int) (Runtime.getRuntime().maxMemory() / HeapSizeConfig.MEGA);
    }

    private static boolean isMaxHeapArgument( String line )
    {
//        return Pattern.matches( MAX_HEAP_SIZE_PREFIX + "+\\d.$", line );
        return line.startsWith( MAX_HEAP_SIZE_PREFIX );
    }
    
    public static void main( String[] args )
    {
        System.out.println( isMaxHeapArgument( "-Xmx123M" ) );
    }

    private String normalize( String string )
    {
        return string.trim();
    }

    private Integer parseJvmHeapSize( String rawValue )
    {
        // Split in value and suffix
        int suffixStartIndex = rawValue.length();
        for ( int i = 0; i < rawValue.length(); i++ )
        {
            if ( !isDigit( rawValue.charAt( i ) ) )
            {
                suffixStartIndex = i;
                break;
            }
        }
        long value = Integer.parseInt( rawValue.substring( 0, suffixStartIndex ) );
        String suffix = suffixStartIndex < rawValue.length() ?
                rawValue.substring( suffixStartIndex ).toLowerCase() : "";
        if ( suffix.equals( "m" ) )
        {   // We want to return in MEGA, so do nothing
        }
        else if ( suffix.equals( "g" ) )
        {
            value = value * HeapSizeConfig.GIGA / HeapSizeConfig.MEGA;
        }
        else if ( suffix.equals( "" ) )
        {
            // as is
        }
        else
        {
            throw new IllegalArgumentException( "Unknown heap size suffix '" + suffix + "'" );
        }
        return (int) value;
    }

    private String parseHeapArgumentValue( String line )
    {
        return line.substring( MAX_HEAP_SIZE_PREFIX.length() );
    }

    @Override
    public void set( Integer value )
    {
        if ( iniFile == null )
        {
            throw new IllegalStateException( "Not writable" );
        }
        
        List<String> lines = iniFile.exists() ? toList( asIterable( iniFile ) ) : new ArrayList<String>();
        ListIterator<String> iterator = lines.listIterator();
        boolean replaced = false;
        String rawValue = MAX_HEAP_SIZE_PREFIX + value + "m";
        while ( iterator.hasNext() )
        {
            String item = normalize( iterator.next() );
            if ( isMaxHeapArgument( item ) )
            {
                iterator.set( rawValue );
                replaced = true;
            }
        }
        
        if ( !replaced )
        {
            lines.add( rawValue );
        }
        
        writeToFile( lines );
    }

    private void writeToFile( List<String> lines )
    {
        PrintStream writer = null;
        try
        {
            writer = new PrintStream( iniFile );
            for ( String line : lines )
            {
                writer.println( line );
            }
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            if ( writer != null )
            {
                writer.close();
            }
        }
    }

    @Override
    public boolean isWritable()
    {
        return iniFile != null;
    }
}
