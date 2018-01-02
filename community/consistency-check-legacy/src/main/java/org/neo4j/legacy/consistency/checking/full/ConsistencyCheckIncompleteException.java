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
package org.neo4j.legacy.consistency.checking.full;

import java.io.PrintStream;
import java.io.PrintWriter;

import org.neo4j.helpers.ProcessFailureException;

public class ConsistencyCheckIncompleteException extends Exception
{
    public ConsistencyCheckIncompleteException( Exception cause )
    {
        super( "Full consistency check did not complete", cause );
    }

    @Override
    public void printStackTrace( PrintStream s )
    {
        super.printStackTrace( s );
        printMultiCause( getCause(), new PrintWriter( s, true ) );
    }

    @Override
    public void printStackTrace( PrintWriter s )
    {
        super.printStackTrace( s );
        printMultiCause( getCause(), s );
    }

    private static void printMultiCause( Throwable cause, PrintWriter writer )
    {
        if ( cause instanceof ProcessFailureException )
        {
            ((ProcessFailureException) cause).printAllCauses( writer );
        }
    }
}
