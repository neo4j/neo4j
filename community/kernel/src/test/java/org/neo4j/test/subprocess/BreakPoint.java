/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import java.util.concurrent.CountDownLatch;

public abstract class BreakPoint implements DebuggerDeadlockCallback
{
    public BreakPoint( Class<?> type, String method, Class<?>... args )
    {
        this.type = type.getName();
        this.method = method;
        this.args = new String[args.length];
        for ( int i = 0; i < args.length; i++ )
        {
            this.args[i] = args[i].getName();
        }
    }

    protected abstract void callback( DebugInterface debug ) throws KillSubProcess;

    @Override
    public void deadlock( DebuggedThread thread )
    {
        throw new SubProcess.DeadlockDetectedError();
    }

    final String type;
    private final String method;
    private final String[] args;
    volatile boolean enabled = false;
    private @SuppressWarnings( "restriction" )
    com.sun.jdi.request.EventRequest request = null;

    @SuppressWarnings( "restriction" )
    public synchronized BreakPoint enable()
    {
        this.enabled = true;
        if ( request != null ) request.enable();
        return this;
    }

    @SuppressWarnings( "restriction" )
    public synchronized BreakPoint disable()
    {
        this.enabled = false;
        if ( request != null ) request.disable();
        return this;
    }

    @SuppressWarnings( "restriction" )
    synchronized void setRequest( com.sun.jdi.request.EventRequest request )
    {
        this.request = request;
        this.request.setEnabled( enabled );
    }

    @SuppressWarnings( "restriction" )
    void setup( com.sun.jdi.ReferenceType type )
    {
        com.sun.jdi.request.EventRequestManager erm = type.virtualMachine().eventRequestManager();
        for ( @SuppressWarnings( "hiding" ) com.sun.jdi.Method method : type.methodsByName( this.method ) )
        {
            if ( matches( method.name(), method.argumentTypeNames() ) )
            {
                setRequest( erm.createBreakpointRequest( method.location() ) );
                return;
            }
        }
    }

    boolean matches( String name, List<String> argNames )
    {
        if ( !name.equals( method ) ) return false;
        if ( argNames.size() != args.length ) return false;
        Iterator<String> names = argNames.iterator();
        for ( int i = 0; i < args.length; i++ )
        {
            if ( !args[i].equals( names.next() ) ) return false;
        }
        return true;
    }

    public static BreakPoint stacktrace( final PrintStream out, Class<?> type, String method, Class<?>... args )
    {
        return new BreakPoint( type, method, args )
        {
            @Override
            protected void callback( DebugInterface debug )
            {
                out.println( "Debugger stacktrace" );
                debug.printStackTrace( out );
            }
        };
    }
    
    public static BreakPoint thatCrashesTheProcess( final CountDownLatch crashNotification,
            final int letNumberOfCallsPass, Class<?> type, String method, Class<?>... args )
    {
        return new BreakPoint( type, method, args )
        {
            private volatile int numberOfCalls;
            
            @Override
            protected void callback( DebugInterface debug ) throws KillSubProcess
            {
                if ( ++numberOfCalls <= letNumberOfCallsPass )
                {
                    return;
                }
                
                debug.thread().suspend( null );
                this.disable();
                crashNotification.countDown();
                throw KillSubProcess.withExitCode( -1 );
            }
        };
    }
}
