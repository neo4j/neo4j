/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.lock.trace;

import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.DefaultTracerFactory;
import org.neo4j.scheduler.JobScheduler;

public class TestLocksTracerFactory extends DefaultTracerFactory
{
    public TestLocksTracerFactory()
    {
    }

    @Override
    public String getImplementationName()
    {
        return "slaveLocksTracer";
    }

    @Override
    public LockTracer createLockTracer( Monitors monitors, JobScheduler jobScheduler )
    {
        return new RecordingLockTracer();
    }
}
