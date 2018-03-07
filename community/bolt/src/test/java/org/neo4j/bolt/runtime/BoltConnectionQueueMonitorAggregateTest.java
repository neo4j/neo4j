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
package org.neo4j.bolt.runtime;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.bolt.v1.runtime.Job;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BoltConnectionQueueMonitorAggregateTest
{
    private BoltConnection connection = mock( BoltConnection.class );

    @Test
    public void shouldCallEnqueuedOnSingleMonitor()
    {
        Job job = mock( Job.class );
        BoltConnectionQueueMonitor monitor = mock( BoltConnectionQueueMonitor.class );
        BoltConnectionQueueMonitorAggregate monitorAggregate = new BoltConnectionQueueMonitorAggregate( monitor );

        monitorAggregate.enqueued( connection, job );

        verify( monitor ).enqueued( connection, job );
    }

    @Test
    public void shouldCallDrainedOnSingleMonitor()
    {
        Collection<Job> batch = new ArrayList<>();
        BoltConnectionQueueMonitor monitor = mock( BoltConnectionQueueMonitor.class );
        BoltConnectionQueueMonitorAggregate monitorAggregate = new BoltConnectionQueueMonitorAggregate( monitor );

        monitorAggregate.drained( connection, batch );

        verify( monitor ).drained( connection, batch );
    }

    @Test
    public void shouldCallEnqueuedOnEachMonitor()
    {
        Job job = mock( Job.class );
        BoltConnectionQueueMonitor monitor1 = mock( BoltConnectionQueueMonitor.class );
        BoltConnectionQueueMonitor monitor2 = mock( BoltConnectionQueueMonitor.class );
        BoltConnectionQueueMonitorAggregate monitorAggregate = new BoltConnectionQueueMonitorAggregate( monitor1, monitor2 );

        monitorAggregate.enqueued( connection, job );

        verify( monitor1 ).enqueued( connection, job );
        verify( monitor2 ).enqueued( connection, job );
    }

    @Test
    public void shouldCallDrainedOnEachMonitor()
    {
        Collection<Job> batch = new ArrayList<>();
        BoltConnectionQueueMonitor monitor1 = mock( BoltConnectionQueueMonitor.class );
        BoltConnectionQueueMonitor monitor2 = mock( BoltConnectionQueueMonitor.class );
        BoltConnectionQueueMonitorAggregate monitorAggregate = new BoltConnectionQueueMonitorAggregate( monitor1, monitor2 );

        monitorAggregate.drained( connection, batch );

        verify( monitor1 ).drained( connection, batch );
        verify( monitor2 ).drained( connection, batch );
    }
}
