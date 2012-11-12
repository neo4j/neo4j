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
import java.util.Iterator;
import java.util.List;

@SuppressWarnings( "restriction" )
public class DebuggedThread
{
    private final com.sun.jdi.ThreadReference thread;
    private final SubProcess.DebugDispatch debug;

    DebuggedThread( SubProcess.DebugDispatch debug, com.sun.jdi.ThreadReference thread )
    {
        this.debug = debug;
        this.thread = thread;
    }
    
    @Override
    public String toString()
    {
        return "DebuggedThread[" + debug.handler + " " + thread.name() + "]";
    }

    public DebuggedThread suspend( DebuggerDeadlockCallback callback )
    {
        thread.suspend();
        debug.suspended( thread, callback );
        return this;
    }

    public DebuggedThread resume()
    {
        thread.resume();
        debug.resume( thread );
        return this;
    }

    public String getLocal( int offset, String name )
    {
        try
        {
            com.sun.jdi.StackFrame frame = thread.frames().get( offset );
            com.sun.jdi.LocalVariable variable = frame.visibleVariableByName( name );
            return frame.getValue( variable ).toString();
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    public StackTraceElement[] getStackTrace()
    {
        try
        {
            List<com.sun.jdi.StackFrame> frames = thread.frames();
            StackTraceElement[] trace = new StackTraceElement[frames.size()];
            Iterator<com.sun.jdi.StackFrame> iter = frames.iterator();
            for ( int i = 0; iter.hasNext(); i++ )
            {
                com.sun.jdi.Location loc = iter.next().location();
                com.sun.jdi.Method method = loc.method();
                String fileName;
                try
                {
                    fileName = loc.sourceName();
                }
                catch ( com.sun.jdi.AbsentInformationException e )
                {
                    fileName = null;
                }
                trace[i] = new StackTraceElement( method.declaringType().name(), method.name(), fileName,
                        loc.lineNumber() );
            }
            return trace;
        }
        catch ( com.sun.jdi.IncompatibleThreadStateException e )
        {
            return new StackTraceElement[0];
        }
    }

    public String name()
    {
        return thread.name();
    }

    public void printStackTrace( PrintStream out )
    {
        out.println( name() );
        for ( StackTraceElement trace : getStackTrace() )
        {
            out.println( "\tat " + trace );
        }
    }
}