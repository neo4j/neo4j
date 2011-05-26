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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.junit.After;
import org.junit.Before;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class AbstractSubProcessTestBase
{
    private final TargetDirectory target;
    protected final Instance[] instances;

    public AbstractSubProcessTestBase()
    {
        this( 1 );
    }

    protected AbstractSubProcessTestBase( int instances )
    {
        this.instances = new Instance[instances];
        this.target = TargetDirectory.forTest( getClass() );
    }

    protected final void run( Task task )
    {
        for ( Instance instance : instances )
        {
            if ( instance != null ) instance.run( task );
        }
    }

    protected final void restart()
    {
        for ( Instance instance : instances )
        {
            if ( instance != null ) instance.restart();
        }
    }

    /**
     * @param id the id of the instance to get breakpoints for
     */
    protected SubProcessBreakPoint[] breakpoints( int id )
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
            instances[i] = prototype.start( bootstrap( i ), breakpoints( i ) );
        }
        for ( Instance instance : instances )
        {
            if ( instance != null ) instance.awaitStarted();
        }
    }

    protected Bootstrapper bootstrap( int id ) throws IOException
    {
        return new Bootstrapper( this, id );
    }

    @After
    public final void stopSubprocesses()
    {
        synchronized ( instances )
        {
            for ( int i = 0; i < instances.length; i++ )
            {
                Instance instance = instances[i];
                if ( instance != null ) SubProcess.stop( instance );
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
    protected static class Bootstrapper implements Serializable
    {
        protected final String storeDir;

        protected Bootstrapper( AbstractSubProcessTestBase test, int instance ) throws IOException
        {
            this.storeDir = test.target.directory( "graphdb." + instance, true ).getCanonicalPath();
        }

        protected AbstractGraphDatabase startup()
        {
            return new EmbeddedGraphDatabase( storeDir );
        }
    }

    @SuppressWarnings( { "hiding", "serial" } )
    private static class SubInstance extends SubProcess<Instance, Bootstrapper> implements Instance
    {
        private volatile AbstractGraphDatabase graphdb;
        private static final AtomicReferenceFieldUpdater<SubInstance, AbstractGraphDatabase> GRAPHDB = AtomicReferenceFieldUpdater
                .newUpdater( SubInstance.class, AbstractGraphDatabase.class, "graphdb" );
        private volatile Bootstrapper bootstrap;

        @Override
        protected synchronized void startup( Bootstrapper bootstrap ) throws Throwable
        {
            this.bootstrap = bootstrap;
            graphdb = bootstrap.startup();
        }

        @Override
        public void awaitStarted() throws InterruptedException
        {
            while ( graphdb == null )
            {
                Thread.sleep( 1 );
            }
        }

        @Override
        public void run( Task task )
        {
            task.run( graphdb );
        }

        @Override
        protected void shutdown()
        {
            AbstractGraphDatabase graphdb;
            graphdb = GRAPHDB.getAndSet( this, null );
            this.bootstrap = null;
            if ( graphdb != null ) graphdb.shutdown();
            super.shutdown();
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
}
