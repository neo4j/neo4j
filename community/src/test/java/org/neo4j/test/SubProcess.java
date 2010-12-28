/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.rmi.server.RemoteObject;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class SubProcess<T, P> implements Serializable
{
    private interface NoInterface
    {
    }

    private final Class<T> t;

    @SuppressWarnings( "unchecked" )
    public SubProcess()
    {
        if ( getClass().getSuperclass() != SubProcess.class )
        {
            throw new ClassCastException( SubProcess.class.getName()
                                          + " may only be extended one level " );
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
        if ( t == Object.class ) t = (Class<T>) (Class) NoInterface.class;
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
            throw new ClassCastException( getClass().getName()
                                          + " must implement declared interface " + t );
        }
    }

    public T start( P parameter )
    {
        DispatcherTrapImpl callback;
        try
        {
            callback = new DispatcherTrapImpl( this, parameter );
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( "Failed to create local RMI endpoint.", e );
        }
        ProcessBuilder builder = new ProcessBuilder( "java", "-cp",
                System.getProperty( "java.class.path" ), SubProcess.class.getName(),
                serialize( callback ) );
        Process process;
        try
        {
            process = builder.start();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to start sub process", e );
        }
        String pid = getPid( process );
        pipe( "[" + toString() + ":" + pid + "] ", process.getErrorStream(), System.err );
        pipe( "[" + toString() + ":" + pid + "] ", process.getInputStream(),
                System.out );
        Dispatcher dispatcher = callback.get( process );
        return t.cast( Proxy.newProxyInstance( t.getClassLoader(), new Class[] { t },//
                new Handler( t, dispatcher, process ) ) );
    }

    protected abstract void startup( P parameter );

    protected void shutdown()
    {
        System.exit( 0 );
    }

    public static void stop( Object subprocess )
    {
        ( (Handler) Proxy.getInvocationHandler( subprocess ) ).stop();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( "Needs to be started from "
                                                + SubProcess.class.getName() );
        }
        DispatcherTrap trap = deserialize( args[0] );
        SubProcess<?, Object> subProcess = trap.getSubProcess();
        subProcess.startup( trap.trap( new DispatcherImpl( subProcess ) ) );
    }

    private static final Field PID;
    static
    {
        Field pid;
        try
        {
            pid = ( (Class<?>) Class.forName( "java.lang.UNIXProcess" ) ).getDeclaredField( "pid" );
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
                int available = source.available();
                if ( available != 0 )
                {
                    byte[] data = new byte[available /*- ( available % 2 )*/];
                    source.read( data );
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
            }
            catch ( IOException e )
            {
                if ( line.length() > 0 )
                {
                    line.append( '\n' );
                    print();
                }
                return false;
            }
            return true;
        }

        private void print()
        {
            target.print( prefix + line.toString() );
            line = new StringBuilder();
        }
    }

    private static class PipeThread extends Thread
    {
        final CopyOnWriteArrayList<PipeTask> tasks = new CopyOnWriteArrayList<PipeTask>();

        @Override
        public void run()
        {
            while ( true )
            {
                List<PipeTask> done = new ArrayList<PipeTask>();
                for ( PipeTask task : tasks )
                {
                    if ( !task.pipe() )
                    {
                        done.add( task );
                    }
                }
                if ( !done.isEmpty() ) tasks.removeAll( done );
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

    private static void pipe( final String prefix, final InputStream source,
            final PrintStream target )
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

        Dispatcher get( Process process )
        {
            while ( dispatcher == null )
            {
                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                    return null;
                }
            }
            return dispatcher;
        }

        public synchronized Object trap( Dispatcher dispatcher )
        {
            if ( this.dispatcher != null )
                throw new IllegalStateException( "Dispatcher already trapped!" );
            this.dispatcher = dispatcher;
            return parameter;
        }

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

        Object dispatch( String name, String[] types, Object[] args ) throws RemoteException,
                Throwable;
    }

    private static class Handler implements InvocationHandler
    {
        private final Dispatcher dispatcher;
        private final Process process;
        private final Class<?> type;

        Handler( Class<?> type, Dispatcher dispatcher, Process process )
        {
            this.type = type;
            this.dispatcher = dispatcher;
            this.process = process;
        }

        int stop()
        {
            try
            {
                dispatcher.stop();
            }
            catch ( RemoteException e )
            {
                process.destroy();
            }
            try
            {
                return process.waitFor();
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                return 0;
            }
        }

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
                    if ( method.getName().equals( "toString" ) )
                    {
                        return dispatch( method, args );
                    }
                    return method.invoke( this, args );
                }
                else
                {
                    throw new UnsupportedOperationException( method.toString() );
                }
            }
            catch ( RemoteException ex )
            {
                throw new IllegalStateException( "Subprocess connection disrupted", ex );
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

        public Object dispatch( String name, String[] types, Object[] args )
                throws RemoteException, Throwable
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

        public void stop() throws RemoteException
        {
            subprocess.shutdown();
        }
    }
}
