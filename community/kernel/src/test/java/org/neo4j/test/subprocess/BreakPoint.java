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

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.Predicate;

public abstract class BreakPoint implements DebuggerDeadlockCallback
{
    public enum Event
    {
        ENTRY,
        EXIT
    }

    public BreakPoint( Class<?> type, String method, Class<?>... args )
    {
        this(Event.ENTRY,type,method,args);
    }

    public BreakPoint( Event event, Class<?> type, String method, Class<?>... args )
    {
        assert methodExists( type, method, args );
        this.event = event;
        this.type = type.getName();
        this.method = method;
        this.args = new String[args.length];
        for ( int i = 0; i < args.length; i++ )
        {
            this.args[i] = args[i].getName();
        }
    }

    private static boolean methodExists( Class<?> type, String method, Class<?>[] args )
    {
        try
        {
            type.getDeclaredMethod( method, args );
            return true;
        }
        catch ( Exception e )
        {
            throw new AssertionError( e );
        }
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( "BreakPoint[" );
        result.append( type ).append( '#' ).append( method ).append( '(' );
        for ( int i = 0; i < args.length; i++ )
        {
            if ( i > 0 ) result.append( ',' );
            result.append( args[i] );
        }
        return result.append( ")]" ).toString();
    }

    public final int invocationCount()
    {
        return count.get();
    }

    public int invocationCount( int value )
    {
        return count.getAndSet( value );
    }

    public final int resetInvocationCount()
    {
        return invocationCount( 0 );
    }

    final void invoke( DebugInterface debug ) throws KillSubProcess
    {
        count.incrementAndGet();
        callback( debug );
    }

    protected abstract void callback( DebugInterface debug ) throws KillSubProcess;

    @Override
    public void deadlock( DebuggedThread thread )
    {
        SubProcess.DebugDispatch.defaultCallback.deadlock( thread );
    }

    final Event event;
    final String type;
    private final String method;
    private final String[] args;
    private final AtomicInteger count = new AtomicInteger();
    volatile boolean enabled = false;
    private @SuppressWarnings( "restriction" )
    com.sun.jdi.request.EventRequest request = null;

    @SuppressWarnings( "restriction" )
    public synchronized boolean isEnabled()
    {
        if ( request != null ) return request.isEnabled();
        return enabled;
    }

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
                switch (event)
                {
                case ENTRY:
                    setRequest( erm.createBreakpointRequest( method.location() ) );
                    return;
                case EXIT:
                    com.sun.jdi.request.MethodExitRequest request = erm.createMethodExitRequest();
                    request.addClassFilter( type );
                    setRequest( request );
                }
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
    
    public static final Predicate<StackTraceElement[]> ALL = new Predicate<StackTraceElement[]>()
    {
        @Override
        public boolean accept( StackTraceElement[] item )
        {
            return true;
        }
    };
    
    private static Predicate<StackTraceElement[]> reversed( final Predicate<StackTraceElement[]> predicate )
    {
        return new Predicate<StackTraceElement[]>()
        {
            @Override
            public boolean accept( StackTraceElement[] item )
            {
                return !predicate.accept( item );
            }
        };
    }
    
    public static Predicate<StackTraceElement[]> stackTraceMustContainClass( final Class<?> cls )
    {
        return new Predicate<StackTraceElement[]>()
        {
            @Override
            public boolean accept( StackTraceElement[] item )
            {
                for ( StackTraceElement element : item )
                    if ( element.getClassName().equals( cls.getName() ) )
                        return true;
                return false;
            }
        };
    }

    public static Predicate<StackTraceElement[]> stackTraceMustNotContainClass( final Class<?> cls )
    {
        return reversed( stackTraceMustContainClass( cls ) );
    }
    
    public static BreakPoint thatCrashesTheProcess( final CountDownLatch crashNotification,
            final int letNumberOfCallsPass, Class<?> type, String method, Class<?>... args )
    {
        return thatCrashesTheProcess( Event.ENTRY, crashNotification, letNumberOfCallsPass, ALL, type, method, args );
    }

    public static BreakPoint thatCrashesTheProcess( Event event, final CountDownLatch crashNotification,
            final int letNumberOfCallsPass, final Predicate<StackTraceElement[]> stackTraceMustContain, Class<?> type,
            String method, Class<?>... args )
    {
        return new BreakPoint( event, type, method, args )
        {
            private volatile int numberOfCalls;

            @Override
            protected void callback( DebugInterface debug ) throws KillSubProcess
            {
                if ( ++numberOfCalls <= letNumberOfCallsPass )
                    return;

                if ( !stackTraceMustContain.accept( debug.thread().getStackTrace() ) )
                    return;
                
                debug.thread().suspend( null );
                this.disable();
                crashNotification.countDown();
                throw KillSubProcess.withExitCode( -1 );
            }
        };
    }
}
