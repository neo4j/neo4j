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
package org.neo4j.kernel.lifecycle;

import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.assertEquals;

/**
 * Test LifeSupport lifecycle transitions
 */
public class LifeSupportTest
{
    @Test
    public void testOkLifecycle()
        throws LifecycleException
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        LifecycleMock instance2 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.init();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus());

        lifeSupport.stop();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus());

        lifeSupport.shutdown();
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance1.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance2.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance3.getStatus());
    }

    @Test()
    public void testFailingInit()
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        Exception initThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( initThrowable, null, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        try
        {
            lifeSupport.init();
            Assert.fail(  );
        }
        catch( LifecycleException throwable )
        {
            assertEquals( initThrowable, throwable.getCause() );
        }
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN , instance1.getStatus());
        assertEquals( LifecycleStatus.NONE , instance2.getStatus() );
        assertEquals( LifecycleStatus.NONE , instance3.getStatus() );
    }

    @Test()
    public void testFailingStart()
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        Exception startThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( null, startThrowable, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        try
        {
            lifeSupport.start();
            Assert.fail(  );
        }
        catch( LifecycleException throwable )
        {
            assertEquals( startThrowable, throwable.getCause());
        }
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED , instance3.getStatus() );
    }

    @Test()
    public void testFailingStartAndFailingStop()
    {
        LifeSupport lifeSupport = newLifeSupport();

        Exception stopThrowable = new Exception();
        LifecycleMock instance1 = new LifecycleMock( null, null, stopThrowable, null );
        lifeSupport.add( instance1 );
        Exception startThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( null, startThrowable, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        try
        {
            lifeSupport.start();
            Assert.fail(  );
        }
        catch( LifecycleException throwable )
        {
            assertEquals( stopThrowable, throwable.getCause());
            assertEquals( startThrowable, throwable.getCause().getCause().getCause() );
        }

        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.STOPPED , instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED , instance3.getStatus() );
    }

    @Test()
    public void testFailingStop()
        throws LifecycleException
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        Exception stopThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( null, null, stopThrowable, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.start();

        try
        {
            lifeSupport.stop();
            Assert.fail(  );
        }
        catch( LifecycleException throwable )
        {
            assertEquals( stopThrowable, throwable.getCause());
        }
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED , instance3.getStatus() );
    }

    @Test()
    public void testFailingShutdown()
        throws LifecycleException
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        Exception shutdownThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( null, null, null, shutdownThrowable );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.start();

        try
        {
            lifeSupport.shutdown();
            Assert.fail(  );
        }
        catch( LifecycleException throwable )
        {
            assertEquals( shutdownThrowable, throwable.getCause());
        }
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN , instance1.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN , instance2.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN , instance3.getStatus() );
    }

    @Test
    public void testRestartOk()
        throws LifecycleException
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        LifecycleMock instance2 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.init();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus());

        lifeSupport.stop();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus());

        lifeSupport.shutdown();
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance1.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance2.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance3.getStatus());
    }

    @Test
    public void testAddInstanceWhenInitInitsInstance()
        throws LifecycleException
    {
        LifeSupport support = newLifeSupport();

        support.init();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );

        support.add( instance1 );

        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus() );

        assertEquals( LifecycleStatus.NONE, instance1.transitions.get( 0 ) );
        assertEquals( 2, instance1.transitions.size() );
    }

    @Test
    public void testAddInstanceWhenStartedStartsInstance()
        throws LifecycleException
    {
        LifeSupport support = newLifeSupport();

        support.init();
        support.start();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );

        support.add( instance1 );

        assertEquals( LifecycleStatus.STARTED, instance1.getStatus() );

        assertEquals( LifecycleStatus.NONE, instance1.transitions.get( 0 ) );
        assertEquals( LifecycleStatus.STOPPED, instance1.transitions.get( 1 ) );

        assertEquals( 3, instance1.transitions.size() );
    }

    @Test
    public void testAddInstanceWhenStoppedInitsInstance()
        throws LifecycleException
    {
        LifeSupport support = newLifeSupport();

        support.init();
        support.start();
        support.stop();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );

        support.add( instance1 );

        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus() );

        assertEquals( LifecycleStatus.NONE, instance1.transitions.get( 0 ) );
        assertEquals( LifecycleStatus.STOPPED, instance1.transitions.get( 1 ) );

        assertEquals( 2, instance1.transitions.size() );
    }

    @Test
    public void testAddInstanceWhenShutdownDoesNotAffectInstance()
        throws LifecycleException
    {
        LifeSupport support = newLifeSupport();

        support.init();
        support.start();
        support.stop();
        support.shutdown();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );

        support.add( instance1 );

        assertEquals( LifecycleStatus.NONE, instance1.getStatus() );

        assertEquals( 1, instance1.transitions.size() );
    }

    public class LifecycleMock
        implements Lifecycle
    {
        Throwable initThrowable;
        Throwable startThrowable;
        Throwable stopThrowable;
        Throwable shutdownThrowable;

        List<LifecycleStatus> transitions;

        public LifecycleMock( Throwable initThrowable,
                              Throwable startThrowable,
                              Throwable stopThrowable,
                              Throwable shutdownThrowable
        )
        {
            this.initThrowable = initThrowable;
            this.startThrowable = startThrowable;
            this.stopThrowable = stopThrowable;
            this.shutdownThrowable = shutdownThrowable;

            transitions = new LinkedList<LifecycleStatus>();
            transitions.add( LifecycleStatus.NONE );
        }

        @Override
        public void init()
            throws Throwable
        {
            if (initThrowable != null)
                throw initThrowable;

            transitions.add(LifecycleStatus.STOPPED);
        }

        @Override
        public void start()
            throws Throwable
        {
            if (startThrowable != null)
                throw startThrowable;

            transitions.add(LifecycleStatus.STARTED);
        }

        @Override
        public void stop()
            throws Throwable
        {
            transitions.add(LifecycleStatus.STOPPED);

            if (stopThrowable != null)
                throw stopThrowable;
        }

        @Override
        public void shutdown()
            throws Throwable
        {
            transitions.add(LifecycleStatus.SHUTDOWN);

            if (shutdownThrowable != null)
                throw shutdownThrowable;
        }

        public LifecycleStatus getStatus()
        {
            return transitions.get( transitions.size() - 1 );
        }
    }

    private LifeSupport newLifeSupport()
    {
        return new LifeSupport( StringLogger.DEV_NULL );
    }
}
