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
package org.neo4j.test.subprocess;

import java.io.PrintStream;

@SuppressWarnings( "restriction" )
public class DebugInterface
{
    private final com.sun.jdi.event.LocatableEvent event;
    private final SubProcess.DebugDispatch debug;

    DebugInterface( SubProcess.DebugDispatch debug, com.sun.jdi.event.LocatableEvent event )
    {
        this.debug = debug;
        this.event = event;
    }

    public boolean matchCallingMethod( int offset, Class<?> owner, String method )
    {
        try
        {
            com.sun.jdi.Location location = event.thread().frame( offset ).location();
            if ( owner != null )
            {
                if ( !owner.getName().equals( location.declaringType().name() ) ) return false;
            }
            if ( method != null )
            {
                if ( !method.equals( location.method().name() ) ) return false;
            }
            return true;
        }
        catch ( com.sun.jdi.IncompatibleThreadStateException e )
        {
            return false;
        }
    }

    public DebuggedThread thread()
    {
        return new DebuggedThread( debug, event.thread() );
    }

    public void printStackTrace( PrintStream out )
    {
        thread().printStackTrace(out);
    }
}