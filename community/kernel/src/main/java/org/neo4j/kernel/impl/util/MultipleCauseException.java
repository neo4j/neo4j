/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultipleCauseException extends Exception
{
    private static final long serialVersionUID = -5556701516106141749L;
    private static final String ALSO_CAUSED_BY = "Also caused by: ";
    private final List<Throwable> causes = new ArrayList<Throwable>();

    public MultipleCauseException( String message, Throwable firstCause )
    {
        super( message, firstCause );
        causes.add( firstCause );
    }
    
    public List<Throwable> getCauses()
    {
        return causes;
    }

    public void addCause( Throwable cause )
    {
        causes.add( cause );
    }

    @Override
    public void printStackTrace( PrintStream out )
    {
        super.printStackTrace( out );
        printAllButFirstCauseStackTrace( out );
    }

    @Override
    public void printStackTrace( PrintWriter out )
    {
        super.printStackTrace( out );
        printAllButFirstCauseStackTrace( out );
    }

    private void printAllButFirstCauseStackTrace( PrintStream out )
    {
        Iterator<Throwable> causeIterator = causes.iterator();
        if ( causeIterator.hasNext() )
        {
            causeIterator.next(); // Skip first (already printed by default
                                  // PrintStackTrace)
            while ( causeIterator.hasNext() )
            {
                out.print( ALSO_CAUSED_BY );
                causeIterator.next().printStackTrace( out );
            }
        }
    }

    private void printAllButFirstCauseStackTrace( PrintWriter out )
    {
        Iterator<Throwable> causeIterator = causes.iterator();
        if ( causeIterator.hasNext() )
        {
            causeIterator.next(); // Skip first (already printed by default
                                  // PrintStackTrace)
            while ( causeIterator.hasNext() )
            {
                out.print( ALSO_CAUSED_BY );
                causeIterator.next().printStackTrace( out );
            }
        }
    }
}
