/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.test.subprocess;

import java.util.Arrays;
import java.util.Iterator;

public class SuspendedThreadsException extends Exception implements Iterable<String>
{
    private final String[] threadNames;

    SuspendedThreadsException( String... threadNames )
    {
        super( message( threadNames ) );
        this.threadNames = threadNames.clone();
    }

    private static String message( String[] threadName )
    {
        if ( threadName == null || threadName.length == 0 )
            throw new IllegalArgumentException( "No thread names given" );
        if ( threadName.length == 1 )
        {
            return "The \"" + threadName[0] + "\" thread is still suspended.";
        }
        else
        {
            StringBuilder message = new StringBuilder( "The following threads are still suspended" );
            String sep = ": ";
            for ( String name : threadName )
            {
                message.append( sep ).append( '"' ).append( name ).append( '"' );
                sep = ", ";
            }
            return message.toString();
        }
    }

    @Override
    public Iterator<String> iterator()
    {
        return Arrays.asList( threadNames ).iterator();
    }
}
