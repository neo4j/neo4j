/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.DeadlockDetectedException;

import static org.neo4j.kernel.impl.locking.Locks.Client;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

@Ignore("Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite.")
public class DeadlockCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public DeadlockCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @After
    public void shutdown()
    {
        threadA.interrupt();
        threadB.interrupt();
        threadC.interrupt();
    }

    @Test
    public void shouldDetectTwoClientExclusiveDeadlock() throws Exception
    {
        assertDetectsDeadlock(
                acquireExclusive( clientA, NODE, 1l ),
                acquireExclusive( clientB, NODE, 2l ),

                acquireExclusive( clientB, NODE, 1l ),
                acquireExclusive( clientA, NODE, 2l ) );
    }

    @Test
    public void shouldDetectThreeClientExclusiveDeadlock() throws Exception
    {
        assertDetectsDeadlock(
                acquireExclusive( clientA, NODE, 1l ),
                acquireExclusive( clientB, NODE, 2l ),
                acquireExclusive( clientC, NODE, 3l ),

                acquireExclusive( clientB, NODE, 1l ),
                acquireExclusive( clientC, NODE, 2l ),
                acquireExclusive( clientA, NODE, 3l ) );
    }

    @Test
    public void shouldDetectMixedExclusiveAndSharedDeadlock() throws Exception
    {
        assertDetectsDeadlock(

                acquireShared( clientA, NODE, 1l ),
                acquireExclusive( clientB, NODE, 2l ),

                acquireExclusive( clientB, NODE, 1l ),
                acquireShared( clientA, NODE, 2l ) );
    }

    private void assertDetectsDeadlock( LockCommand... commands )
    {
        List<Pair<Client, Future<Object>>> calls = new ArrayList<>();
        for ( LockCommand command : commands )
        {
            calls.add( Pair.of( command.client(), command.call() ) );
        }

        long timeout = System.currentTimeMillis() + (1000 * 10);
        while(System.currentTimeMillis() < timeout)
        {
            for ( Pair<Client, Future<Object>> call : calls )
            {
                try
                {
                    call.other().get(1, TimeUnit.MILLISECONDS);
                }
                catch ( ExecutionException e )
                {
                    if(e.getCause() instanceof DeadlockDetectedException)
                    {
                        return;
                    }
                    else
                    {
                        throw new RuntimeException( e );
                    }
                }
                catch ( InterruptedException | TimeoutException e )
                {
                    // Fine, we're just looking for deadlocks, clients may still be waiting for things
                }
            }
        }

        throw new AssertionError( "Failed to detect deadlock. Expected lock manager to detect deadlock, " +
                "but none of the clients reported any deadlocks." );
    }
}
