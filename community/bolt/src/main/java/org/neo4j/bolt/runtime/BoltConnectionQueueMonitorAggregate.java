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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.neo4j.bolt.v1.runtime.Job;

public class BoltConnectionQueueMonitorAggregate implements BoltConnectionQueueMonitor
{
    private final List<BoltConnectionQueueMonitor> monitors;

    public BoltConnectionQueueMonitorAggregate( BoltConnectionQueueMonitor... monitors )
    {
        this.monitors = Arrays.asList( monitors );
    }

    @Override
    public void enqueued( BoltConnection to, Job job )
    {
        monitors.forEach( m -> m.enqueued( to, job ) );
    }

    @Override
    public void drained( BoltConnection from, Collection<Job> batch )
    {
        monitors.forEach( m -> m.drained( from, batch ) );
    }
}
