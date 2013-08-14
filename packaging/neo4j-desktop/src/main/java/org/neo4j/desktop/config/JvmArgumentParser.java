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

import static java.lang.Character.isDigit;

/**
 * JVM Heap size argument parser. Values returned and accepted are in megabytes.
 */
public class JvmArgumentParser
{
    private static final String MAX_HEAP_SIZE_PREFIX = "-Xmx";
    
    public boolean isMaxHeap( String line )
    {
//        return Pattern.matches( MAX_HEAP_SIZE_PREFIX + "+\\d.$", line );
        return line.startsWith( MAX_HEAP_SIZE_PREFIX );
    }
    
    private String parseHeapArgumentValue( String line )
    {
        return line.substring( MAX_HEAP_SIZE_PREFIX.length() );
    }

    public int parseMaxHeap( String fullXmxArgument )
    {
        // Split in value and suffix
        String rawValue = parseHeapArgumentValue( fullXmxArgument );
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
            value = value * OsSpecificHeapSizeConfig.GIGA / OsSpecificHeapSizeConfig.MEGA;
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
    
    public String produceMaxHeapArgument( int megaBytes )
    {
        return MAX_HEAP_SIZE_PREFIX + megaBytes + "M";
    }

    public int getCurrentMaxHeap()
    {
        return (int) (Runtime.getRuntime().maxMemory() / OsSpecificHeapSizeConfig.MEGA);
    }
}
