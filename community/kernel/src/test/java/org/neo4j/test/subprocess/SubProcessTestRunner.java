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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.neo4j.helpers.Exceptions;
import org.neo4j.test.subprocess.ForeignBreakpoints.BreakpointDef;
import org.neo4j.test.subprocess.SubProcess.DebugDispatch;

public class SubProcessTestRunner extends BlockJUnit4ClassRunner
{
    public SubProcessTestRunner( Class<?> test ) throws InitializationError
    {
        super( test );
    }

    private final Map<String, BreakPoint> breakpoints = new HashMap<String, BreakPoint>();
    private volatile TestRunnerDispatcher dispatcher;
    //private final SequencingManager seqManager = null;
    private final Task.Executor taskExecutor = new Task.Executor()
    {
        @Override
        public void submit( final Task<?> task )
        {
            new Thread()
            {
                @Override
                public void run()
                {
                    try
                    {
                        dispatcher.submit( task );
                    }
                    catch ( Throwable e )
                    {
                        e.printStackTrace();
                    }
                }
            }.start();
        }
    };

    @Override
    protected void collectInitializationErrors( java.util.List<Throwable> errors )
    {
        super.collectInitializationErrors( errors );

        for ( FrameworkMethod handler : getTestClass().getAnnotatedMethods( BreakpointHandler.class ) )
        {
            handler.validatePublicVoid( /*static:*/true, errors );
        }
        for ( FrameworkMethod beforeDebugged : getTestClass().getAnnotatedMethods( BeforeDebuggedTest.class ) )
        {
            beforeDebugged.validatePublicVoidNoArg( /*static:*/true, errors );
        }
    }

    private void startSubProcess( RunNotifier notifier ) throws Throwable
    {
        EachTestNotifier eachNotifier = new EachTestNotifier( notifier, getDescription() );
        NotifierImpl remoteNotifier = new NotifierImpl( eachNotifier );
        this.dispatcher = new TestProcess( getTestClass().getJavaClass().getName() ).start( remoteNotifier,
                breakpoints() );
    }

    private void stopSubProcess()
    {
        try
        {
            SubProcess.stop( dispatcher );
        }
        finally
        {
            dispatcher = null;
        }
    }

    @Override
    protected Statement classBlock( final RunNotifier notifier )
    {
        final Statement children = childrenInvoker( notifier );
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                startSubProcess( notifier );
                try
                {
                    children.evaluate();
                }
                finally
                {
                    stopSubProcess();
                }
            }
        };
    }

    private BreakPoint[] breakpoints() throws Throwable
    {
        if ( breakpoints.isEmpty() )
        {
            synchronized ( breakpoints )
            {
                if ( breakpoints.isEmpty() )
                {
                    List<Throwable> failures = new ArrayList<Throwable>();
                    final Object CLAIMED = new Object();
                    Map<String, Object> bpDefs = new HashMap<String, Object>();
                    ForeignBreakpoints foreign = getTestClass().getJavaClass().getAnnotation( ForeignBreakpoints.class );
                    if ( foreign != null ) for ( BreakpointDef def : foreign.value() )
                    {
                        String name = def.name();
                        if ( name.isEmpty() ) name = def.method();
                        if ( null != bpDefs.put( name, def ) )
                            failures.add( new Exception( "Multiple definitions of the breakpoint \"" + name + "\"" ) );
                    }
                    for ( FrameworkMethod method : getTestClass().getAnnotatedMethods( BreakpointTrigger.class ) )
                    {
                        String name = method.getAnnotation( BreakpointTrigger.class ).value();
                        if ( name.isEmpty() ) name = method.getName();
                        if ( null != bpDefs.put( name, method ) )
                            failures.add( new Exception( "Multiple definitions of the breakpoint \"" + name + "\"" ) );
                    }
                    for ( FrameworkMethod handler : getTestClass().getAnnotatedMethods( BreakpointHandler.class ) )
                    {
                        for ( String name : handler.getAnnotation( BreakpointHandler.class ).value() )
                        {
                            Object bp = bpDefs.get( name );
                            if ( bp == null )
                            {
                                failures.add( new Exception( "No such breakpoint: \"" + name + "\", referenced from: "
                                                             + handler ) );
                            }
                            else if ( bp == CLAIMED )
                            {
                                failures.add( new Exception( "Multiple handlers for breakpoint: \"" + name
                                                             + "\", referenced from: " + handler ) );
                            }
                            else if ( bp instanceof BreakpointDef )
                            {
                                try
                                {
                                    for ( BreakpointDispatcher dispatch : createForeignBreakpoints( (BreakpointDef) bp,
                                            handler ) )
                                    {
                                        breakpoints.put( name, dispatch );
                                    }
                                }
                                catch ( Exception exc )
                                {
                                    failures.add( exc );
                                }
                            }
                            else if ( bp instanceof FrameworkMethod )
                            {
                                breakpoints.put( name, new BreakpointDispatcher(
                                        ( (FrameworkMethod) bp ).getAnnotation( BreakpointTrigger.class ).on(),
                                        ( (FrameworkMethod) bp ).getMethod().getDeclaringClass(),
                                        ( (FrameworkMethod) bp ).getMethod(), handler ) );
                            }
                            else
                            {
                                failures.add( new Exception( "Internal error, unknown breakpoint def: " + bp ) );
                            }
                            bpDefs.put( name, CLAIMED );
                        }
                    }
                    if ( bpDefs.size() != breakpoints.size() ) for ( Object bp : bpDefs.values() )
                    {
                        if ( bp != CLAIMED ) failures.add( new Exception( "Unhandled breakpoint: " + bp ) );
                    }
                    if ( !failures.isEmpty() )
                    {
                        if ( failures.size() == 1 ) throw failures.get( 0 );
                        throw new MultipleFailureException( failures );
                    }
                }
            }
        }
        return breakpoints.values().toArray( new BreakPoint[breakpoints.size()] );
    }

    Iterable<BreakpointDispatcher> createForeignBreakpoints( BreakpointDef def, FrameworkMethod handler ) throws Exception
    {
        Class<?> type = Class.forName( def.type() );
        List<BreakpointDispatcher> result = new ArrayList<BreakpointDispatcher>( 1 );
        for ( Method method : type.getDeclaredMethods() ) if ( method.getName().equals( def.method() ) )
        {
            result.add( new BreakpointDispatcher( def.on(), type, method, handler ) );
        }
        if ( result.isEmpty() ) throw new Exception( "No such method: " + def );
        return result;
    }

    private class BreakpointDispatcher extends BreakPoint
    {
        private final FrameworkMethod handler;

        BreakpointDispatcher( BreakPoint.Event event, Class<?> type, Method method, FrameworkMethod handler )
        {
            super( event, type, method.getName(), method.getParameterTypes() );
            this.handler = handler;
        }

        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            Class<?>[] types = handler.getMethod().getParameterTypes();
            Annotation[][] annotations = handler.getMethod().getParameterAnnotations();
            Object[] params = new Object[types.length];
            BUILD_PARAM_LIST: for ( int i = 0; i < types.length; i++ )
            {
                if ( types[i] == DebugInterface.class )
                    params[i] = debug;
                else if ( types[i] == BreakPoint.class )
                {
                    for ( Annotation annotation : annotations[i] )
                    {
                        if ( BreakpointHandler.class.isInstance( annotation ) )
                        {
                            String[] value = ( (BreakpointHandler) annotation ).value();
                            if ( value.length == 1 )
                            {
                                params[i] = breakpoints.get( value[0] );
                                continue BUILD_PARAM_LIST;
                            }
                        }
                    }
                    params[i] = this;
                }
                /*
                else if ( types[i] == SequencingManager.class )
                    params[i] = seqManager;
                */
                else if ( types[i] == Task.Executor.class )
                    params[i] = taskExecutor;
                else
                    params[i] = null;
            }
            try
            {
                handler.invokeExplosively( null, params );
            }
            catch ( Throwable exception )
            {
                throw Exceptions.launderedException( KillSubProcess.class, exception );
            }
        }
    }

    @Override
    protected Statement methodBlock( final FrameworkMethod method )
    {
        Statement statement = new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                enableBreakpoints( method.getAnnotation( EnabledBreakpoints.class ) );
                dispatcher.run( method.getName() );
            }
        };
        List<FrameworkMethod> befores = getTestClass().getAnnotatedMethods( BeforeDebuggedTest.class );
        if ( !befores.isEmpty() ) statement = new RunBefores( statement, befores, null );
        return statement;
    }

    private void enableBreakpoints( EnabledBreakpoints breakpoints )
    {
        Set<String> enabled = new HashSet<String>();
        if ( breakpoints != null ) for ( String name : breakpoints.value() )
        {
            enabled.add( name );
        }
        for ( Map.Entry<String, BreakPoint> entry : this.breakpoints.entrySet() )
        {
            BreakPoint bp = entry.getValue();
            ( enabled.remove( entry.getKey() ) ? bp.enable() : bp.disable() ).resetInvocationCount();
        }
        if ( !enabled.isEmpty() ) throw new IllegalArgumentException( "Unknown breakpoints: " + enabled );
    }

    private void verifyBreakpointState() throws SuspendedThreadsException
    {
        DebugDispatch debugger = SubProcess.DebugDispatch.get( dispatcher );
        // if there are no breakpoints we will have no debugger
        DebuggedThread[] threads = (debugger == null) ? new DebuggedThread[0] : debugger.suspendedThreads();
        if ( threads.length != 0 )
        {
            String[] names = new String[threads.length];
            for ( int i = 0; i < threads.length; i++ )
            {
                names[i] = threads[i].name();
                threads[i].resume();
            }
            throw new SuspendedThreadsException( names );
        }
    }

    interface TestRunnerDispatcher
    {
        void submit( Task<?> task ) throws Throwable;

        void run( String methodName ) throws Throwable;
    }

    private interface RemoteRunNotifier extends Remote
    {
        void failure( Throwable exception ) throws RemoteException;

        void checkPostConditions() throws RemoteException, Throwable;
    }

    private class NotifierImpl extends UnicastRemoteObject implements RemoteRunNotifier
    {
        private static final long serialVersionUID = 1L;
        private final EachTestNotifier notifier;

        NotifierImpl( EachTestNotifier notifier ) throws RemoteException
        {
            super();
            this.notifier = notifier;
        }

        @Override
        public void failure( Throwable exception ) throws RemoteException
        {
            notifier.addFailure( exception );
        }

        @Override
        public void checkPostConditions() throws Throwable
        {
            verifyBreakpointState();
        }
    }

    private static class RemoteRunListener extends RunListener
    {
        private final RemoteRunNotifier remote;

        public RemoteRunListener( RemoteRunNotifier remote )
        {
            this.remote = remote;
        }

        @Override
        public void testFailure( Failure failure ) throws Exception
        {
            remote.failure( failure.getException() );
        }
    }

    public static class RunTerminatedException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        private RunTerminatedException()
        {
            // all instances are created here
        }
    }

    private static final Object TERMINATED = new Object();

    private static class TestProcess extends SubProcess<TestRunnerDispatcher, RemoteRunNotifier> implements
            TestRunnerDispatcher
    {
        private static final long serialVersionUID = 1L;
        private final String className;

        public TestProcess(String className)
        {
            this.className = className;
        }
        private volatile Object runner;

        @Override
        protected void startup( RemoteRunNotifier remote ) throws Throwable
        {
            try
            {
                RunNotifier notifier = new RunNotifier();
                notifier.addListener( new RemoteRunListener( remote ) );
                new RemoteTestRunner( this, remote, Class.forName( className ) ).run( notifier );
            }
            catch ( Throwable failure )
            {
                runner = failure;
                throw failure;
            }
        }

        @Override
        public String toString()
        {
            return className.substring( className.lastIndexOf( '.' ) + 1);
        }

        @Override
        protected void shutdown( boolean normal )
        {
            try
            {
                Object runner = this.runner;
                if ( runner instanceof RemoteTestRunner ) ( (RemoteTestRunner) runner ).terminate();
            }
            finally
            {
                this.runner = new RunTerminatedException();
            }
            super.shutdown( normal );
        }

        @Override
        public void run( String methodName ) throws Throwable
        {
            runner().run( methodName );
        }

        @Override
        public void submit( Task<?> task ) throws Throwable
        {
            runner().run( task );
        }

        private RemoteTestRunner runner() throws Throwable
        {
            for ( Object runner = this.runner;; runner = this.runner )
            {
                if ( runner instanceof RemoteTestRunner )
                {
                    return (RemoteTestRunner) runner;
                }
                else if ( runner instanceof Throwable )
                {
                    throw (Throwable) runner;
                }
                Thread.sleep( 1 ); // yield
            }
        }
    }

    private static class RemoteTestRunner extends BlockJUnit4ClassRunner
    {
        private final TestProcess testProcess;
        private final RemoteRunNotifier host;
        private volatile Object testMethod;
        private volatile Object test;

        RemoteTestRunner( TestProcess testProcess, RemoteRunNotifier host, Class<?> test ) throws InitializationError
        {
            super( test );
            this.testProcess = testProcess;
            this.host = host;
        }

        void terminate()
        {
            testMethod = TERMINATED;
        }

        @Override
        protected Statement childrenInvoker( RunNotifier notifier )
        {
            return new Statement()
            {
                @Override
                public void evaluate()
                {
                    testProcess.runner = RemoteTestRunner.this;
                    for ( Object test = testMethod;; test = testMethod )
                    {
                        if ( test == null || test instanceof Throwable )
                        {
                            try
                            {
                                Thread.sleep( 1 ); // yield
                            }
                            catch ( InterruptedException e )
                            {
                                testMethod = e;
                                break;
                            }
                        }
                        else if ( test instanceof FrameworkMethod )
                        {
                            try
                            {
                                methodBlock( (FrameworkMethod) test ).evaluate();
                                testMethod = null;
                            }
                            catch ( Throwable e )
                            {
                                testMethod = e;
                            }
                        }
                        else break; // received poison pill
                    }
                }
            };
        }

        @Override
        protected Statement methodBlock( FrameworkMethod method )
        {
            final Statement statement = super.methodBlock( method );
            return statement;
        }

        @Override
        protected Statement methodInvoker( FrameworkMethod method, Object test )
        {
            this.test = test;
            final Statement statement = super.methodInvoker( method, test );
            return new Statement()
            {
                @Override
                public void evaluate() throws Throwable
                {
                    statement.evaluate();
                    host.checkPostConditions();
                }
            };
        }

        private Map<String, FrameworkMethod> methods;

        void run( String methodName ) throws Throwable
        {
            if ( methods == null )
            {
                Map<String, FrameworkMethod> map = new HashMap<String, FrameworkMethod>();
                for ( FrameworkMethod method : getChildren() )
                {
                    map.put( method.getName(), method );
                }
                methods = map;
            }
            testMethod = methods.get( methodName );
            for ( Object result = testMethod;; result = testMethod )
            {
                if ( result instanceof FrameworkMethod )
                {
                    Thread.sleep( 1 ); // yield
                }
                else if ( result instanceof Throwable )
                {
                    throw (Throwable) result;
                }
                else return; // success
            }
        }

        <T> void run( Task<T> task )
        {
            task.run( inject( typeOf( task ) ) );
        }

        private <T> T inject( Class<T> type )
        {
            if ( type == null ) /*could not find concrete parameter type*/return null;
            Object test = this.test;
            if ( type.isInstance( test ) ) return type.cast( test );
            return null; // TODO: what other things should we be able to inject into tasks?
        }

        @SuppressWarnings( "unchecked" )
        private static <T> Class<T> typeOf( Task<T> task )
        {
            Class<?> taskType = task.getClass();
            while ( taskType != Object.class )
            {
                for ( Type type : taskType.getGenericInterfaces() )
                {
                    if ( type instanceof ParameterizedType )
                    {
                        ParameterizedType paramType = (ParameterizedType) type;
                        if ( paramType.getRawType() == Task.class )
                        {
                            Type param = paramType.getActualTypeArguments()[0];
                            if ( param.getClass() == Class.class ) return (Class<T>) param;
                        }
                    }
                }
                taskType = taskType.getSuperclass();
            }
            return null;
        }
    }
}
