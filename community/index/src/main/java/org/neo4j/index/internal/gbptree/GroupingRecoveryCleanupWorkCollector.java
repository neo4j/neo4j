/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.internal.gbptree;

import java.util.LinkedList;
import java.util.Queue;

public class GroupingRecoveryCleanupWorkCollector implements RecoveryCleanupWorkCollector
{
    private final Queue<CleanupJob> jobs = new LinkedList<>();
    private volatile boolean started = false;

    @Override
    public void add( CleanupJob job )
    {
        assert !started : "Tried to add cleanup job after started";
        jobs.add( job );
    }

    @Override
    public void run()
    {
        assert !started : "Tried to start cleanup job more than once";
        started = true;
        CleanupJob job;
        while ( (job = jobs.poll()) != null )
        {
            job.run();
        }
    }
}
