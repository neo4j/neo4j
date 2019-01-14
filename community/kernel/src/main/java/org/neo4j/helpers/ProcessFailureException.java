/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;

/**
 * @deprecated Exception is not in use anymore
 */
@Deprecated
public class ProcessFailureException extends Exception
{
    public static final class Entry implements Serializable
    {
        private final String part;
        private final Throwable failure;

        public Entry( String part, Throwable failure )
        {
            this.part = part;
            this.failure = failure;
        }

        @Override
        public String toString()
        {
            return "In '" + part + "': " + failure;
        }
    }

    private final Entry[] causes;

    public ProcessFailureException( List<Entry> causes )
    {
        super( "Monitored process failed" + message( causes ), cause( causes ) );
        this.causes = causes.toArray( new Entry[0] );
    }

    private static String message( List<Entry> causes )
    {
        if ( causes.isEmpty() )
        {
            return ".";
        }
        if ( causes.size() == 1 )
        {
            return " in '" + causes.get( 0 ).part + "'.";
        }
        StringBuilder result = new StringBuilder( ":" );
        for ( Entry entry : causes )
        {
            result.append( "\n\t" ).append( entry );
        }
        return result.toString();
    }

    @SuppressWarnings( "ThrowableResultOfMethodCallIgnored" )
    private static Throwable cause( List<Entry> causes )
    {
        return causes.size() >= 1 ? causes.get( 0 ).failure : null;
    }

    @Override
    public void printStackTrace( PrintStream s )
    {
        super.printStackTrace( s );
        printAllCauses( new PrintWriter( s, true ) );
    }

    @Override
    public void printStackTrace( PrintWriter s )
    {
        super.printStackTrace( s );
        printAllCauses( s );
    }

    public void printAllCauses( PrintWriter writer )
    {
        if ( getCause() == null )
        {
            for ( Entry entry : causes )
            {
                entry.failure.printStackTrace( writer );
            }
            writer.flush();
        }
    }
}
