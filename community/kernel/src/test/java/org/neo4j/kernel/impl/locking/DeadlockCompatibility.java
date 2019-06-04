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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.lock.LockTracer;

import static org.neo4j.lock.ResourceTypes.NODE;

abstract class DeadlockCompatibility extends LockCompatibilityTestSupport
{
    DeadlockCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    void shouldDetectTwoClientExclusiveDeadlock() throws Exception
    {
        acquireExclusive( clientA, LockTracer.NONE, NODE, 1L ).call().get();
        acquireExclusive( clientB, LockTracer.NONE, NODE, 2L ).call().get();

        assertDetectsDeadlock(
                acquireExclusive( clientB, LockTracer.NONE, NODE, 1L ),
                acquireExclusive( clientA, LockTracer.NONE, NODE, 2L ) );
    }

    @Test
    void shouldDetectThreeClientExclusiveDeadlock() throws Exception
    {
        acquireExclusive( clientA, LockTracer.NONE, NODE, 1L ).call().get();
        acquireExclusive( clientB, LockTracer.NONE, NODE, 2L ).call().get();
        acquireExclusive( clientC, LockTracer.NONE, NODE, 3L ).call().get();

        assertDetectsDeadlock(
                acquireExclusive( clientB, LockTracer.NONE, NODE, 1L ),
                acquireExclusive( clientC, LockTracer.NONE, NODE, 2L ),
                acquireExclusive( clientA, LockTracer.NONE, NODE, 3L ) );
    }

    @Test
    void shouldDetectMixedExclusiveAndSharedDeadlock() throws Exception
    {
        acquireShared( clientA, LockTracer.NONE, NODE, 1L ).call().get();
        acquireExclusive( clientB, LockTracer.NONE, NODE, 2L ).call().get();

        assertDetectsDeadlock(
                acquireExclusive( clientB, LockTracer.NONE, NODE, 1L ),
                acquireShared( clientA, LockTracer.NONE, NODE, 2L ) );
    }

    private void assertDetectsDeadlock( LockCommand... commands )
    {
        List<Future<Void>> calls = Arrays.stream( commands )
                .map( LockCommand::call )
                .collect( Collectors.toList() );

        long timeout = System.currentTimeMillis() + (1000 * 10);
        while ( System.currentTimeMillis() < timeout )
        {
            for ( Future<Void> call : calls )
            {
                if ( tryDetectDeadlock( call ) )
                {
                    return;
                }
            }
        }

        throw new AssertionError( "Failed to detect deadlock. Expected lock manager to detect deadlock, " +
                "but none of the clients reported any deadlocks." );
    }

    private boolean tryDetectDeadlock( Future<Void> call )
    {
        try
        {
            call.get( 1, TimeUnit.MILLISECONDS );
        }
        catch ( ExecutionException e )
        {
            if ( e.getCause() instanceof DeadlockDetectedException )
            {
                return true;
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
        return false;
    }
}
