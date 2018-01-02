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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.neo4j.graphdb.Resource;
import org.neo4j.test.OtherThreadExecutor.WaitDetails;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;

public class ParallelizationCoordinatorTest
{
    @Test
    public void shouldSerializeNonParallelizableBatches() throws Exception
    {
        // GIVEN
        ParallelizationCoordinator coordinator = new ParallelizationCoordinator();

        // WHEN
        Future<Resource> r2Future;
        try ( Resource r1 = coordinator.coordinate( false ) )
        {
            r2Future = t2.execute( coordinate( coordinator, false ) );
            waitUntilAwaitingLock( t2 );
        }

        // THEN
        t2.execute( close( r2Future.get() ) ).get();
    }

    @Test
    public void shouldParallelizeBatches() throws Exception
    {
        // GIVEN
        ParallelizationCoordinator coordinator = new ParallelizationCoordinator();

        // WHEN
        Resource r1 = coordinator.coordinate( true );
        Resource r2 = coordinator.coordinate( true );

        // THEN
        r1.close();
        r2.close();
    }

    @Test
    public void shouldHaveNonParallelizableBatchAwaitPreviousParallelizable() throws Exception
    {
        // GIVEN
        ParallelizationCoordinator coordinator = new ParallelizationCoordinator();
        Resource r1 = coordinator.coordinate( true );
        Resource r2 = coordinator.coordinate( true );
        Future<Resource> r3Future = t2.execute( coordinate( coordinator, false ) );
        waitUntilAwaitingLock( t2 );

        // WHEN the previous parallelizable batches are done
        r2.close();
        r1.close();

        // THEN we should be able to continue
        t2.execute( close( r3Future.get() ) ).get();
    }

    private void waitUntilAwaitingLock( OtherThreadRule<Void> thread ) throws TimeoutException
    {
        while ( true )
        {
            WaitDetails details = thread.get().waitUntilWaiting();
            if ( details.isAt( ParallelizationCoordinator.class, "coordinate" ) )
            {
                break;
            }
        }
    }

    private WorkerCommand<Void,Resource> coordinate( final ParallelizationCoordinator coordinator,
            final boolean parallelizable )
    {
        return new WorkerCommand<Void,Resource>()
        {
            @Override
            public Resource doWork( Void state ) throws Exception
            {
                return coordinator.coordinate( parallelizable );
            }
        };
    }

    private WorkerCommand<Void,Void> close( final Resource resource )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                resource.close();
                return null;
            }
        };
    }

    public final @Rule OtherThreadRule<Void> t2 = new OtherThreadRule<>();
}
