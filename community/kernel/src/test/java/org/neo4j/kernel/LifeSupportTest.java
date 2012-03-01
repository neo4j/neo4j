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

package org.neo4j.kernel;

import org.junit.Assert;
import org.junit.Test;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.*;

/**
 * Test LifeSupport lifecycle transitions
 */
public class LifeSupportTest
{
    @Test
    public void testOkLifecycle()
        throws LifecycleException
    {
        LifeSupport lifeSupport = new LifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        LifecycleMock instance2 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.init();
        assertEquals( LifeSupport.Status.STOPPED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance1.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance2.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifeSupport.Status.STARTED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance1.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance2.getStatus() );
        assertEquals( LifeSupport.Status.STARTED, instance3.getStatus());

        lifeSupport.stop();
        assertEquals( LifeSupport.Status.STOPPED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance1.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance2.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifeSupport.Status.STARTED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance1.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance2.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance3.getStatus());

        lifeSupport.shutdown();
        assertEquals( LifeSupport.Status.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.SHUTDOWN, instance1.getStatus());
        assertEquals( LifeSupport.Status.SHUTDOWN, instance2.getStatus());
        assertEquals( LifeSupport.Status.SHUTDOWN, instance3.getStatus());
    }

    @Test()
    public void testFailingInit()
    {
        LifeSupport lifeSupport = new LifeSupport();

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
        assertEquals( LifeSupport.Status.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.SHUTDOWN , instance1.getStatus());
        assertEquals( LifeSupport.Status.NONE , instance2.getStatus() );
        assertEquals( LifeSupport.Status.NONE , instance3.getStatus() );
    }

    @Test()
    public void testFailingStart()
    {
        LifeSupport lifeSupport = new LifeSupport();

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
        assertEquals( LifeSupport.Status.STOPPED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STOPPED , instance1.getStatus());
        assertEquals( LifeSupport.Status.STOPPED , instance2.getStatus() );
        assertEquals( LifeSupport.Status.STOPPED , instance3.getStatus() );
    }

    @Test()
    public void testFailingStartAndFailingStop()
    {
        LifeSupport lifeSupport = new LifeSupport();

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

        lifeSupport.dump( StringLogger.SYSTEM );

        assertEquals( LifeSupport.Status.STOPPED, lifeSupport.getStatus() );
        assertEquals( LifeSupport.Status.STOPPED , instance1.getStatus());
        assertEquals( LifeSupport.Status.STOPPED , instance2.getStatus() );
        assertEquals( LifeSupport.Status.STOPPED , instance3.getStatus() );
    }

    @Test()
    public void testFailingStop()
        throws LifecycleException
    {
        LifeSupport lifeSupport = new LifeSupport();

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
        assertEquals( LifeSupport.Status.STOPPED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STOPPED , instance1.getStatus());
        assertEquals( LifeSupport.Status.STOPPED , instance2.getStatus() );
        assertEquals( LifeSupport.Status.STOPPED , instance3.getStatus() );
    }

    @Test()
    public void testFailingShutdown()
        throws LifecycleException
    {
        LifeSupport lifeSupport = new LifeSupport();

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
        assertEquals( LifeSupport.Status.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.SHUTDOWN , instance1.getStatus());
        assertEquals( LifeSupport.Status.SHUTDOWN , instance2.getStatus() );
        assertEquals( LifeSupport.Status.SHUTDOWN , instance3.getStatus() );
    }

    @Test
    public void testRestartOk()
        throws LifecycleException
    {
        LifeSupport lifeSupport = new LifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        LifecycleMock instance2 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.init();
        assertEquals( LifeSupport.Status.STOPPED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance1.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance2.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifeSupport.Status.STARTED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance1.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance2.getStatus() );
        assertEquals( LifeSupport.Status.STARTED, instance3.getStatus());

        lifeSupport.stop();
        assertEquals( LifeSupport.Status.STOPPED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance1.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance2.getStatus());
        assertEquals( LifeSupport.Status.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifeSupport.Status.STARTED, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance1.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance2.getStatus());
        assertEquals( LifeSupport.Status.STARTED, instance3.getStatus());

        lifeSupport.shutdown();
        assertEquals( LifeSupport.Status.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifeSupport.Status.SHUTDOWN, instance1.getStatus());
        assertEquals( LifeSupport.Status.SHUTDOWN, instance2.getStatus());
        assertEquals( LifeSupport.Status.SHUTDOWN, instance3.getStatus());
    }

    public class LifecycleMock
        implements Lifecycle
    {
        Throwable initThrowable;
        Throwable startThrowable;
        Throwable stopThrowable;
        Throwable shutdownThrowable;
        
        LifeSupport.Status status = LifeSupport.Status.NONE;

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
        }

        @Override
        public void init()
            throws Throwable
        {
            if (initThrowable != null)
                throw initThrowable;

            status = LifeSupport.Status.STOPPED;
        }

        @Override
        public void start()
            throws Throwable
        {
            if (startThrowable != null)
                throw startThrowable;

            status = LifeSupport.Status.STARTED;
        }

        @Override
        public void stop()
            throws Throwable
        {
            status = LifeSupport.Status.STOPPED;

            if (stopThrowable != null)
                throw stopThrowable;
        }

        @Override
        public void shutdown()
            throws Throwable
        {
            status = LifeSupport.Status.SHUTDOWN;

            if (shutdownThrowable != null)
                throw shutdownThrowable;
        }

        public LifeSupport.Status getStatus()
        {
            return status;
        }
    }
}
