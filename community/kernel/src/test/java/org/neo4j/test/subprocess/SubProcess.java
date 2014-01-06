/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.ServerError;
import java.rmi.server.RemoteObject;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Predicate;
import org.neo4j.test.ProcessStreamHandler;

@SuppressWarnings( "serial" )
public abstract class SubProcess<T, P> implements Serializable
{
    private interface NoInterface
    {
        // Used when no interface is declared
    }

    private final Class<T> t;
    private final transient Predicate<String> classPathFilter;

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    public SubProcess( Predicate<String> classPathFilter )
    {
        if ( getClass().getSuperclass() != SubProcess.class )
        {
            throw new ClassCastException( SubProcess.class.getName() + " may only be extended one level " );
        }
        Class<?> me = getClass();
        while ( me.getSuperclass() != SubProcess.class )
        {
            me = me.getSuperclass();
        }
        Type type = ( (ParameterizedType) me.getGenericSuperclass() ).getActualTypeArguments()[0];
        @SuppressWarnings( { "hiding" } ) Class<T> t;
        if ( type instanceof Class<?> )
        {
            t = (Class<T>) type;
        }
        else if ( type instanceof ParameterizedType )
        {
            t = (Class<T>) ( (ParameterizedType) type ).getRawType();
        }
        else
        {
            throw new ClassCastException( "Illegal type parameter " + type );
        }
        if ( t == Object.class )
        {
            t = (Class) NoInterface.class;
        }
        if ( !t.isInterface() )
        {
            throw new ClassCastException( t + " is not an interface" );
        }
        if ( t.isAssignableFrom( getClass() ) || t == NoInterface.class )
        {
            this.t = t;
        }
        else
        {
            throw new ClassCastException( getClass().getName() + " must implement declared interface " + t );
        }
        this.classPathFilter = classPathFilter;
    }

    public SubProcess()
    {
        this( null );
    }

    public T start( P parameter, BreakPoint... breakpoints )
    {
        DebuggerConnector debugger = null;
        if ( breakpoints != null && breakpoints.length != 0 )
        {
            debugger = new DebuggerConnector( breakpoints );
        }
        DispatcherTrapImpl callback;
        try
        {
            callback = new DispatcherTrapImpl( this, parameter );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( "Failed to create local RMI endpoint.", e );
        }
        Process process;
        String pid;
        DebugDispatch debugDispatch = null;
        Dispatcher dispatcher;
        try
        {
            synchronized ( debugger != null ? DebuggerConnector.class : new Object() )
            {
                if ( debugger != null )
                {
                    process = start( "java", "-Xmx1G", debugger.listen(), "-cp",
                            classPath( System.getProperty( "java.class.path" ) ), SubProcess.class.getName(),
                            serialize( callback ) );
                }
                else
                {
                    process = start( "java", "-Xmx1G", "-cp", classPath( System.getProperty( "java.class.path" ) ),
                            SubProcess.class.getName(), serialize( callback ) );
                }
                pid = getPid( process );
                pipe( "[" + toString() + ":" + pid + "] ", process.getErrorStream(), errorStreamTarget() );
                pipe( "[" + toString() + ":" + pid + "] ", process.getInputStream(), inputStreamTarget() );
                if ( debugger != null )
                {
                    debugDispatch = debugger.connect( toString() + ":" + pid );
                }
            }
            dispatcher = callback.get( process );
        }
        finally
        {
            try
            {
                UnicastRemoteObject.unexportObject( callback, true );
            }
            catch ( RemoteException e )
            {
                e.printStackTrace();
            }
        }
        if ( dispatcher == null )
        {
            throw new IllegalStateException( "failed to start sub process" );
        }
        Handler handler = new Handler( t, dispatcher, process, "<" + toString() + ":" + pid + ">", debugDispatch );
        if ( debugDispatch != null )
        {
            debugDispatch.handler = handler;
        }
        return t.cast( Proxy.newProxyInstance( t.getClassLoader(), new Class[] { t }, live( handler ) ) );
    }
    
    protected PrintStream errorStreamTarget()
    {
        return System.err;
    }
    
    protected PrintStream inputStreamTarget()
    {
        return System.out;
    }

    private String classPath( String parentClasspath )
    {
        if ( classPathFilter == null )
        {
            return parentClasspath;
        }
        StringBuilder result = new StringBuilder();
        for ( String part : parentClasspath.split( File.pathSeparator ) )
        {
            if ( classPathFilter.accept( part ) )
            {
                result.append( result.length() > 0 ? File.pathSeparator : "" ).append( part );
            }
        }
        return result.toString();
    }

    private static Process start( String... args )
    {
        ProcessBuilder builder = new ProcessBuilder( args );
        try
        {
            return builder.start();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to start sub process", e );
        }
    }

    @SuppressWarnings( "restriction" )
    private static class DebuggerConnector
    {
        private static final com.sun.jdi.connect.ListeningConnector connector;
        static
        {
            com.sun.jdi.connect.ListeningConnector first = null;
            for ( com.sun.jdi.connect.ListeningConnector conn : com.sun.jdi.Bootstrap.virtualMachineManager().listeningConnectors() )
            {
                first = conn;
                break;
            }
            connector = first;
        }
        private final Map<String, List<BreakPoint>> breakpoints = new HashMap<>();
        private final Map<String, ? extends com.sun.jdi.connect.Connector.Argument> args;

        DebuggerConnector( BreakPoint[] breakpoints )
        {
            this.args = connector.defaultArguments();
            for ( BreakPoint breakpoint : breakpoints )
            {
                List<BreakPoint> list = this.breakpoints.get( breakpoint.type );
                if ( list == null )
                {
                    this.breakpoints.put( breakpoint.type, list = new ArrayList<>() );
                }
                list.add( breakpoint );
            }
        }

        String listen()
        {
            try
            {
                return String.format( "-agentlib:jdwp=transport=%s,address=%s", connector.transport().name(),
                        connector.startListening( args ) );
            }
            catch ( Exception e )
            {
                throw new UnsupportedOperationException( "Debugger not supported", e );
            }
        }

        DebugDispatch connect( String string )
        {
            final com.sun.jdi.VirtualMachine vm;
            try
            {
                vm = connector.accept( args );
                connector.stopListening( args );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Debugger connection failure", e );
            }
            com.sun.jdi.request.EventRequestManager erm = vm.eventRequestManager();
            TYPES: for ( Map.Entry<String, List<BreakPoint>> entry : breakpoints.entrySet() )
            {
                for ( com.sun.jdi.ReferenceType type : vm.classesByName( entry.getKey() ) )
                {
                    if ( type.name().equals( entry.getKey() ) )
                    {
                        for ( BreakPoint breakpoint : entry.getValue() )
                        {
                            breakpoint.setup( type );
                        }
                        continue TYPES;
                    }
                }
                com.sun.jdi.request.ClassPrepareRequest prepare = erm.createClassPrepareRequest();
                prepare.addClassFilter( entry.getKey() );
                prepare.enable();
            }
            if ( vm.canRequestMonitorEvents() )
            {
                erm.createMonitorContendedEnterRequest().enable();
            }
            DebugDispatch dispatch = new DebugDispatch( vm.eventQueue(), breakpoints );
            new Thread( dispatch, "Debugger: [" + string + "]" ).start();
            return dispatch;
        }
    }

    @SuppressWarnings( "restriction" )
    static class DebugDispatch implements Runnable
    {
        static DebugDispatch get( Object o )
        {
            if ( Proxy.isProxyClass( o.getClass() ) )
            {
                InvocationHandler handler = Proxy.getInvocationHandler( o );
                if ( handler instanceof Handler )
                {
                    return ( (Handler) handler ).debugDispatch;
                }
            }
            throw new IllegalArgumentException( "Not a sub process: " + o );
        }

        volatile Handler handler;
        private final com.sun.jdi.event.EventQueue queue;
        private final Map<String, List<BreakPoint>> breakpoints;
        private final Map<com.sun.jdi.ThreadReference, DebuggerDeadlockCallback> suspended = new HashMap<>();
        static final DebuggerDeadlockCallback defaultCallback = new DebuggerDeadlockCallback()
        {
            @Override
            public void deadlock( DebuggedThread thread )
            {
                throw new DeadlockDetectedError();
            }
        };

        DebugDispatch( com.sun.jdi.event.EventQueue queue, Map<String, List<BreakPoint>> breakpoints )
        {
            this.queue = queue;
            this.breakpoints = breakpoints;
        }

        @Override
        public void run()
        {
            for ( ;; )
            {
                final com.sun.jdi.event.EventSet events;
                try
                {
                    events = queue.remove();
                }
                catch ( InterruptedException e )
                {
                    return;
                }
                Integer exitCode = null;
                try
                {
                    for ( com.sun.jdi.event.Event event : events )
                    {
                        if ( event instanceof com.sun.jdi.event.MonitorContendedEnterEvent )
                        {
                            com.sun.jdi.event.MonitorContendedEnterEvent monitor = (com.sun.jdi.event.MonitorContendedEnterEvent) event;
                            final com.sun.jdi.ThreadReference thread;
                            try
                            {
                                thread = monitor.monitor().owningThread();
                            }
                            catch ( com.sun.jdi.IncompatibleThreadStateException e )
                            {
                                e.printStackTrace();
                                continue;
                            }
                            if ( thread != null && thread.isSuspended() )
                            {
                                DebuggerDeadlockCallback callback = suspended.get( thread );
                                try
                                {
                                    if ( callback != null )
                                    {
                                        callback.deadlock( new DebuggedThread( this, thread ) );
                                    }
                                }
                                catch ( DeadlockDetectedError deadlock )
                                {
                                    @SuppressWarnings( "hiding" ) Handler handler = this.handler;
                                    if ( handler != null )
                                    {
                                        handler.kill( false );
                                    }
                                }
                            }
                        }
                        else if ( event instanceof com.sun.jdi.event.LocatableEvent )
                        {
                            callback( (com.sun.jdi.event.LocatableEvent) event );
                        }
                        else if ( event instanceof com.sun.jdi.event.ClassPrepareEvent )
                        {
                            setup( ( (com.sun.jdi.event.ClassPrepareEvent) event ).referenceType() );
                        }
                        else if ( event instanceof com.sun.jdi.event.VMDisconnectEvent
                                  || event instanceof com.sun.jdi.event.VMDeathEvent )
                        {
                            return;
                        }
                    }
                }
                catch ( KillSubProcess kill )
                {
                    exitCode = kill.exitCode;
                }
                finally
                {
                    if ( exitCode != null )
                    {
                        events.virtualMachine().exit( exitCode );
                    }
                    else
                    {
                        events.resume();
                    }
                }
            }
        }

        private void setup( com.sun.jdi.ReferenceType type )
        {
            List<BreakPoint> list = breakpoints.get( type.name() );
            if ( list == null )
            {
                return;
            }
            for ( BreakPoint breakpoint : list )
            {
                breakpoint.setup( type );
            }
        }

        private void callback( com.sun.jdi.event.LocatableEvent event ) throws KillSubProcess
        {
            List<BreakPoint> list = breakpoints.get( event.location().declaringType().name() );
            if ( list == null )
            {
                return;
            }
            com.sun.jdi.Method method = event.location().method();
            for ( BreakPoint breakpoint : list )
            {
                if ( breakpoint.matches( method.name(), method.argumentTypeNames() ) )
                {
                    if ( breakpoint.enabled )
                    {
                        breakpoint.invoke( new DebugInterface( this, event ) );
                    }
                }
            }
        }

        void suspended( com.sun.jdi.ThreadReference thread, DebuggerDeadlockCallback callback )
        {
            if ( callback == null )
            {
                callback = defaultCallback;
            }
            suspended.put( thread, callback );
        }

        void resume( com.sun.jdi.ThreadReference thread )
        {
            suspended.remove( thread );
        }

        DebuggedThread[] suspendedThreads()
        {
            if ( suspended.isEmpty() )
            {
                return new DebuggedThread[0];
            }
            List<DebuggedThread> threads = new ArrayList<>();
            for ( com.sun.jdi.ThreadReference thread : suspended.keySet() )
            {
                threads.add( new DebuggedThread( this, thread ) );
            }
            return threads.toArray( new DebuggedThread[threads.size()] );
        }
    }

    static class DeadlockDetectedError extends Error
    {
        @Override
        public Throwable fillInStackTrace()
        {
            return this;
        }
    }

    protected abstract void startup( P parameter ) throws Throwable;

    public final void shutdown()
    {
        shutdown( true );
    }

    protected void shutdown( boolean normal )
    {
        System.exit( 0 );
    }

    public static void stop( Object subprocess )
    {
        ( (Handler) Proxy.getInvocationHandler( subprocess ) ).stop( null, 0 );
    }

    public static void stop( Object subprocess, long timeout, TimeUnit unit )
    {
        ( (Handler) Proxy.getInvocationHandler( subprocess ) ).stop( unit, timeout );
    }

    public static void kill( Object subprocess )
    {
        ( (Handler) Proxy.getInvocationHandler( subprocess ) ).kill( true );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    public static void main( String[] args ) throws Throwable
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( "Needs to be started from " + SubProcess.class.getName() );
        }
        DispatcherTrap trap = deserialize( args[0] );
        SubProcess<?, Object> subProcess = trap.getSubProcess();
        subProcess.doStart( trap.trap( new DispatcherImpl( subProcess ) ) );
    }

    private transient volatile boolean alive;

    private void doStart( P parameter ) throws Throwable
    {
        alive = true;
        startup( parameter );
        liveLoop();
    }

    private void doStop( boolean normal )
    {
        alive = false;
        shutdown( normal );
    }

    private void liveLoop() throws Exception
    {
        while ( alive )
        {
            for ( int i = System.in.available(); i >= 0; i-- )
            {
                if ( System.in.read() == -1 )
                {
                    // Parent process exited, die with it
                    doStop( false );
                }
                Thread.sleep( 1 );
            }
        }
    }

    private static final Field PID;
    static
    {
        Field pid;
        try
        {
            pid = Class.forName( "java.lang.UNIXProcess" ).getDeclaredField( "pid" );
            pid.setAccessible( true );
        }
        catch ( Throwable ex )
        {
            pid = null;
        }
        PID = pid;
    }

    private int lastPid = 0;

    private String getPid( Process process )
    {
        if ( PID != null )
        {
            try
            {
                return PID.get( process ).toString();
            }
            catch ( Exception ok )
            {
                // handled by lastPid++
            }
        }
        return Integer.toString( lastPid++ );
    }

    private static class PipeTask
    {
        private final String prefix;
        private final InputStream source;
        private final PrintStream target;
        private StringBuilder line;

        PipeTask( String prefix, InputStream source, PrintStream target )
        {
            this.prefix = prefix;
            this.source = source;
            this.target = target;
            line = new StringBuilder();
        }

        boolean pipe()
        {
            try
            {
                byte[] data = new byte[Math.max( 1, source.available() )];
                int bytesRead = source.read( data );
                if ( bytesRead == -1 )
                {
                    printLastLine();
                    return false;
                }
                if ( bytesRead < data.length )
                {
                    data = Arrays.copyOf( data, bytesRead );
                }
                ByteBuffer chars = ByteBuffer.wrap( data );
                while ( chars.hasRemaining() )
                {
                    char c = (char) chars.get();
                    line.append( c );
                    if ( c == '\n' )
                    {
                        print();
                    }
                }
            }
            catch ( IOException e )
            {
                printLastLine();
                return false;
            }
            return true;
        }

        private void printLastLine()
        {
            if ( line.length() > 0 )
            {
                line.append( '\n' );
                print();
            }
        }

        private void print()
        {
            target.print( prefix + line.toString() );
            line = new StringBuilder();
        }
    }

    private static class PipeThread extends Thread
    {
        {
            setName( getClass().getSimpleName() );
        }
        final CopyOnWriteArrayList<PipeTask> tasks = new CopyOnWriteArrayList<>();

        @Override
        public void run()
        {
            while ( true )
            {
                List<PipeTask> done = new ArrayList<>();
                for ( PipeTask task : tasks )
                {
                    if ( !task.pipe() )
                    {
                        done.add( task );
                    }
                }
                if ( !done.isEmpty() )
                {
                    tasks.removeAll( done );
                }
                if ( tasks.isEmpty() )
                {
                    synchronized ( PipeThread.class )
                    {
                        if ( tasks.isEmpty() )
                        {
                            piper = null;
                            return;
                        }
                    }
                }
                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
            }
        }
    }

    private static PipeThread piper;

    private static void pipe( final String prefix, final InputStream source, final PrintStream target )
    {
        synchronized ( PipeThread.class )
        {
            if ( piper == null )
            {
                piper = new PipeThread();
                piper.start();
            }
            piper.tasks.add( new PipeTask( prefix, source, target ) );
        }
    }

    private interface DispatcherTrap extends Remote
    {
        Object trap( Dispatcher dispatcher ) throws RemoteException;

        SubProcess<?, Object> getSubProcess() throws RemoteException;
    }

    private static class DispatcherTrapImpl extends UnicastRemoteObject implements DispatcherTrap
    {
        private final Object parameter;
        private volatile Dispatcher dispatcher;
        private final SubProcess<?, ?> process;

        DispatcherTrapImpl( SubProcess<?, ?> process, Object parameter ) throws RemoteException
        {
            super();
            this.process = process;
            this.parameter = parameter;
        }

        Dispatcher get( @SuppressWarnings( "hiding" ) Process process )
        {
            while ( dispatcher == null )
            {
                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                }
                try
                {
                    process.exitValue();
                }
                catch ( IllegalThreadStateException e )
                {
                    continue;
                }
                return null;
            }
            return dispatcher;
        }

        @Override
        public synchronized Object trap( @SuppressWarnings( "hiding" ) Dispatcher dispatcher )
        {
            if ( this.dispatcher != null )
            {
                throw new IllegalStateException( "Dispatcher already trapped!" );
            }
            this.dispatcher = dispatcher;
            return parameter;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public SubProcess<?, Object> getSubProcess()
        {
            return (SubProcess<?, Object>) process;
        }
    }

    @SuppressWarnings( "restriction" )
    private static String serialize( DispatcherTrapImpl obj )
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream oos = new ObjectOutputStream( os );
            oos.writeObject( RemoteObject.toStub( obj ) );
            oos.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Broken implementation!", e );
        }
        return new sun.misc.BASE64Encoder().encode( os.toByteArray() );
    }

    @SuppressWarnings( "restriction" )
    private static DispatcherTrap deserialize( String data )
    {
        try
        {
            return (DispatcherTrap) new ObjectInputStream( new ByteArrayInputStream(
                    new sun.misc.BASE64Decoder().decodeBuffer( data ) ) ).readObject();
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    private interface Dispatcher extends Remote
    {
        void stop() throws RemoteException;

        Object dispatch( String name, String[] types, Object[] args ) throws RemoteException, Throwable;
    }

    private static InvocationHandler live( Handler handler )
    {
        try
        {
            synchronized ( Handler.class )
            {
                if ( live == null )
                {
                    final Set<Handler> handlers = live = new HashSet<>();
                    Runtime.getRuntime().addShutdownHook( new Thread()
                    {
                        @Override
                        public void run()
                        {
                            killAll( handlers );
                        }
                    } );
                }
                live.add( handler );
            }
        }
        catch ( UnsupportedOperationException e )
        {
            handler.kill( false );
            throw new IllegalStateException( "JVM is shutting down!" );
        }
        return handler;
    }

    private static void dead( Handler handler )
    {
        synchronized ( Handler.class )
        {
            try
            {
                if ( live != null )
                {
                    live.remove( handler );
                }
            }
            catch ( UnsupportedOperationException ok )
            {
                // ok, already dead
            }
        }
    }

    private static void killAll( Set<Handler> handlers )
    {
        synchronized ( Handler.class )
        {
            if ( !handlers.isEmpty() )
            {
                for ( Handler handler : handlers )
                {
                    try
                    {
                        handler.process.exitValue();
                    }
                    catch ( IllegalThreadStateException e )
                    {
                        handler.kill( false );
                    }
                }
            }
            live = Collections.emptySet();
        }
    }

    private static Set<Handler> live;

    private static class Handler implements InvocationHandler
    {
        private final Dispatcher dispatcher;
        private final Process process;
        private final Class<?> type;
        private final String repr;
        private final DebugDispatch debugDispatch;

        Handler( Class<?> type, Dispatcher dispatcher, Process process, String repr, DebugDispatch debugDispatch )
        {
            this.type = type;
            this.dispatcher = dispatcher;
            this.process = process;
            this.repr = repr;
            this.debugDispatch = debugDispatch;
        }

        @Override
        public String toString()
        {
            return repr;
        }

        void kill( boolean wait )
        {
            process.destroy();
            if ( wait )
            {
                dead( this );
                await( process );
            }
        }

        int stop( TimeUnit unit, long timeout )
        {
            final CountDownLatch latch = new CountDownLatch( unit == null ? 0 : 1 );
            Thread stopper = new Thread()
            {
                @Override
                public void run()
                {
                    latch.countDown();
                    try
                    {
                        dispatcher.stop();
                    }
                    catch ( RemoteException e )
                    {
                        process.destroy();
                    }
                }
            };
            stopper.start();
            try
            {
                latch.await();
                timeout = System.currentTimeMillis() + ( unit == null ? 0 : unit.toMillis( timeout ) );
                while ( stopper.isAlive() && System.currentTimeMillis() < timeout )
                {
                    Thread.sleep( 1 );
                }
            }
            catch ( InterruptedException e )
            {
                // handled by exit
            }
            if ( stopper.isAlive() )
            {
                stopper.interrupt();
            }
            dead( this );
            return await( process );
        }

        private static int await( Process process )
        {
            return new ProcessStreamHandler( process, true ).waitForResult();
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            try
            {
                if ( method.getDeclaringClass() == type )
                {
                    return dispatch( method, args );
                }
                else if ( method.getDeclaringClass() == Object.class )
                {
                    return method.invoke( this, args );
                }
                else
                {
                    throw new UnsupportedOperationException( method.toString() );
                }
            }
            catch ( ServerError ex )
            {
                throw ex.detail;
            }
            catch ( RemoteException ex )
            {
                throw new ConnectionDisruptedException( ex );
            }
        }

        private Object dispatch( Method method, Object[] args ) throws Throwable
        {
            Class<?>[] params = method.getParameterTypes();
            String[] types = new String[params.length];
            for ( int i = 0; i < types.length; i++ )
            {
                types[i] = params[i].getName();
            }
            return dispatcher.dispatch( method.getName(), types, args );
        }
    }

    private static class DispatcherImpl extends UnicastRemoteObject implements Dispatcher
    {
        private transient final SubProcess<?, ?> subprocess;

        protected DispatcherImpl( SubProcess<?, ?> subprocess ) throws RemoteException
        {
            super();
            this.subprocess = subprocess;
        }

        @Override
        public Object dispatch( String name, String[] types, Object[] args ) throws Throwable
        {
            Class<?>[] params = new Class<?>[types.length];
            for ( int i = 0; i < params.length; i++ )
            {
                params[i] = Class.forName( types[i] );
            }
            try
            {
                return subprocess.t.getMethod( name, params ).invoke( subprocess, args );
            }
            catch ( IllegalAccessException e )
            {
                throw new IllegalStateException( e );
            }
            catch ( InvocationTargetException e )
            {
                throw e.getTargetException();
            }
        }

        @Override
        public void stop() throws RemoteException
        {
            subprocess.doStop( true );
        }
    }
}
