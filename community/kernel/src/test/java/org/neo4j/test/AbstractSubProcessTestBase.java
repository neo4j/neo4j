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
package org.neo4j.test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.junit.After;
import org.junit.Before;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.SubProcess;

public class AbstractSubProcessTestBase
{
    private final TargetDirectory target;
    protected final Pair<Instance, BreakPoint[]>[] instances;

    public AbstractSubProcessTestBase()
    {
        this( 1 );
    }

    @SuppressWarnings( "unchecked" )
    protected AbstractSubProcessTestBase( int instances )
    {
        this.instances = new Pair[instances];
        this.target = TargetDirectory.forTest( getClass() );
    }

    protected final void runInThread( Task task )
    {
        run( new ThreadTask( task ) );
    }

    protected final void run( Task task )
    {
        for ( Pair<Instance, BreakPoint[]> instance : instances )
        {
            if ( instance != null ) instance.first().run( task );
        }
    }

    protected final void restart()
    {
        for ( Pair<Instance, BreakPoint[]> instance : instances )
        {
            if ( instance != null ) instance.first().restart();
        }
    }

    /**
     * @param id the id of the instance to get breakpoints for
     */
    protected BreakPoint[] breakpoints( int id )
    {
        return null;
    }

    protected interface Task extends Serializable
    {
        void run( AbstractGraphDatabase graphdb );
    }

    @Before
    public final void startSubprocesses() throws IOException, InterruptedException
    {
        SubInstance prototype = new SubInstance();
        for ( int i = 0; i < instances.length; i++ )
        {
            BreakPoint[] breakPoints = breakpoints( i );
            instances[i] = Pair.of( prototype.start( bootstrap( i ), breakPoints ), breakPoints );
        }
        for ( Pair<Instance, BreakPoint[]> instance : instances )
        {
            if ( instance != null ) instance.first().awaitStarted();
        }
    }

    protected final void killSubprocesses()
    {
        synchronized ( instances )
        {
            for ( int i = 0; i < instances.length; i++ )
            {
                Pair<Instance, BreakPoint[]> instance = instances[i];
                if ( instance != null )
                {
                    Thread.currentThread().interrupt();
                    SubProcess.kill( instance.first() );
                    Thread.interrupted();
                }
                instances[i] = null;
            }
        }
    }
    
    protected void enableAllBreakPoints()
    {
        for ( Pair<Instance, BreakPoint[]> instance : instances )
        {
            for ( BreakPoint breakPoint : instance.other() )
            {
                breakPoint.enable();
            }
        }
    }

    protected Bootstrapper bootstrap( int id ) throws IOException
    {
        return bootstrap( id, new HashMap<String, String>() );
    }

    protected Bootstrapper bootstrap( int id, Map<String, String> dbConfiguration ) throws IOException
    {
        return new Bootstrapper( this, id, dbConfiguration );
    }

    @After
    public final void stopSubprocesses()
    {
        synchronized ( instances )
        {
            for ( int i = 0; i < instances.length; i++ )
            {
                Pair<Instance, BreakPoint[]> instance = instances[i];
                if ( instance != null ) SubProcess.stop( instance.first() );
                instances[i] = null;
            }
        }
    }

    public interface Instance
    {
        void run( Task task );

        void awaitStarted() throws InterruptedException;

        void restart();

        // <T> T getMBean( Class<T> beanType );
    }

    @SuppressWarnings( "serial" )
    public static class Bootstrapper implements Serializable
    {
        protected final String storeDir;
        private final Map<String, String> dbConfiguration;

        public Bootstrapper( AbstractSubProcessTestBase test, int instance )
                                                                               throws IOException
        {
            this( test, instance, Collections.EMPTY_MAP );
        }

        public Bootstrapper( AbstractSubProcessTestBase test, int instance,
                Map<String, String> dbConfiguration ) throws IOException
        {
            this.dbConfiguration = dbConfiguration;
            this.storeDir = test.target.directory( "graphdb." + instance, true ).getCanonicalPath();
        }

        protected AbstractGraphDatabase startup()
        {
            return new EmbeddedGraphDatabase( storeDir, dbConfiguration );
        }

        protected void shutdown( AbstractGraphDatabase graphdb, boolean normal )
        {
            graphdb.shutdown();
        }
    }
    
    protected static Bootstrapper killAwareBootstrapper( AbstractSubProcessTestBase test, int instance,
            Map<String, String> dbConfiguration )
    {
        try
        {
            return new KillAwareBootstrapper( test, instance, dbConfiguration );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    public static class KillAwareBootstrapper extends Bootstrapper
    {
        public KillAwareBootstrapper( AbstractSubProcessTestBase test, int instance,
                Map<String, String> dbConfiguration ) throws IOException
        {
            super( test, instance, dbConfiguration );
        }

        public KillAwareBootstrapper( AbstractSubProcessTestBase test, int instance )
                throws IOException
        {
            super( test, instance );
        }

        @Override
        protected void shutdown( AbstractGraphDatabase graphdb, boolean normal )
        {
            if ( normal ) super.shutdown( graphdb, normal );
        }
    }

    @SuppressWarnings( "serial" )
    private static class ThreadTask implements Task
    {
        private final Task task;

        ThreadTask( Task task )
        {
            this.task = task;
        }

        @Override
        public void run( final AbstractGraphDatabase graphdb )
        {
            new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    task.run( graphdb );
                }
            }, task.toString() ).start();
        }
    }

    @SuppressWarnings( { "hiding", "serial" } )
    private static class SubInstance extends SubProcess<Instance, Bootstrapper> implements Instance
    {
        private volatile AbstractGraphDatabase graphdb;
        private static final AtomicReferenceFieldUpdater<SubInstance, AbstractGraphDatabase> GRAPHDB = AtomicReferenceFieldUpdater
                .newUpdater( SubInstance.class, AbstractGraphDatabase.class, "graphdb" );
        private volatile Bootstrapper bootstrap;
        private volatile Throwable failure;

        @Override
        protected synchronized void startup( Bootstrapper bootstrap )
        {
            this.bootstrap = bootstrap;
            try
            {
                graphdb = bootstrap.startup();
            }
            catch ( Throwable failure )
            {
                this.failure = failure;
            }
        }
        
        @Override
        public void awaitStarted() throws InterruptedException
        {
            while ( graphdb == null )
            {
                Throwable failure = this.failure;
                if ( failure != null ) throw new StartupFailureException( failure );
                Thread.sleep( 1 );
            }
        }

        @Override
        public void run( Task task )
        {
            task.run( graphdb );
        }

        @Override
        protected void shutdown( boolean normal )
        {
            AbstractGraphDatabase graphdb;
            Bootstrapper bootstrap = this.bootstrap;
            graphdb = GRAPHDB.getAndSet( this, null );
            this.bootstrap = null;
            if ( graphdb != null )
            {
                bootstrap.shutdown( graphdb, normal );
            }
            super.shutdown( normal );
        }

        @Override
        public void restart()
        {
            AbstractGraphDatabase graphdb;
            Bootstrapper bootstrap = this.bootstrap;
            while ( ( graphdb = GRAPHDB.getAndSet( this, null ) ) == null )
            {
                if ( ( bootstrap = this.bootstrap ) == null )
                    throw new IllegalStateException( "instance has been shut down" );
            }
            graphdb.shutdown();
            this.graphdb = bootstrap.startup();
        }

        public <T> T getMBean( Class<T> beanType )
        {
            AbstractGraphDatabase graphdb;
            while ( ( graphdb = this.graphdb ) == null )
            {
                if ( this.bootstrap == null ) throw new IllegalStateException( "instance has been shut down" );
            }
            return graphdb.getManagementBean( beanType );
        }
    }

    @SuppressWarnings( "serial" )
    private static class StartupFailureException extends RuntimeException
    {
        StartupFailureException( Throwable failure )
        {
            super( failure );
        }
    }
}
