/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.locking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.Config;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ActorsExtension
@TestDirectoryExtension
public abstract class LockCompatibilityTestSupport
{
    @Inject
    public Actor threadA;

    @Inject
    public Actor threadB;

    @Inject
    public Actor threadC;

    @Inject
    public TestDirectory testDir;

    protected final LockingCompatibilityTestSuite suite;

    protected Locks locks;
    protected Locks.Client clientA;
    protected Locks.Client clientB;
    protected Locks.Client clientC;

    private final Map<Locks.Client, Actor> clientToThreadMap = new HashMap<>();

    public LockCompatibilityTestSupport( LockingCompatibilityTestSuite suite )
    {
        this.suite = suite;
    }

    @BeforeEach
    public void before()
    {
        locks = suite.createLockManager( Config.defaults(), Clocks.systemClock() );
        clientA = locks.newClient();
        clientB = locks.newClient();
        clientC = locks.newClient();

        clientToThreadMap.put( clientA, threadA );
        clientToThreadMap.put( clientB, threadB );
        clientToThreadMap.put( clientC, threadC );
    }

    @AfterEach
    public void after()
    {
        clientA.close();
        clientB.close();
        clientC.close();
        locks.close();
        clientToThreadMap.clear();
    }

    // Utilities

    public abstract class LockCommand implements Runnable
    {
        private final Actor thread;
        private final Locks.Client client;

        LockCommand( Actor thread, Locks.Client client )
        {
            this.thread = thread;
            this.client = client;
        }

        public Future<Void> call()
        {
            return thread.submit( this );
        }

        Future<Void> callAndAssertWaiting()
        {
            Future<Void> otherThreadLock = call();
            try
            {
                thread.untilWaiting();
            }
            catch ( InterruptedException e )
            {
                throw new IllegalStateException( e );
            }
            assertFalse( otherThreadLock.isDone(), "Should not have acquired lock." );
            return otherThreadLock;
        }

        @Override
        public void run()
        {
            doWork( client );
        }

        abstract void doWork( Locks.Client client ) throws AcquireLockTimeoutException;

        public Locks.Client client()
        {
            return client;
        }
    }

    protected LockCommand acquireExclusive(
            final Locks.Client client,
            final LockTracer tracer,
            final ResourceType resourceType,
            final long key )
    {
        return new LockCommand( clientToThreadMap.get( client ), client )
        {
            @Override
            public void doWork( Locks.Client client ) throws AcquireLockTimeoutException
            {
                client.acquireExclusive( tracer, resourceType, key );
            }
        };
    }

    protected LockCommand acquireShared(
            Locks.Client client,
            final LockTracer tracer,
            final ResourceType resourceType,
            final long key )
    {
        return new LockCommand( clientToThreadMap.get( client ), client )
        {
            @Override
            public void doWork( Locks.Client client ) throws AcquireLockTimeoutException
            {
                client.acquireShared( tracer, resourceType, key );
            }
        };
    }

    protected LockCommand release(
            final Locks.Client client,
            final ResourceType resourceType,
            final long key )
    {
        return new LockCommand( clientToThreadMap.get( client ), client )
        {
            @Override
            public void doWork( Locks.Client client )
            {
                client.releaseExclusive( resourceType, key );
            }
        };
    }

    void assertNotWaiting( Locks.Client client, Future<Void> lock )
    {
        assertDoesNotThrow( () -> lock.get( 5, TimeUnit.SECONDS ), "Waiting for lock timed out!" );
    }

    void assertWaiting( Locks.Client client, Future<Void> lock )
    {
        assertThrows( TimeoutException.class, () -> lock.get( 10, TimeUnit.MILLISECONDS ) );
        assertDoesNotThrow( () -> clientToThreadMap.get( client ).untilWaiting() );
    }
}
