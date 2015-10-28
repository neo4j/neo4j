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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;

public class DefaultCheckPointerTracer implements CheckPointTracer, CheckPointerMonitor
{
    private final Clock clock;

    private volatile long startTime = -1;
    private volatile long endTime = -1;
    private volatile long totalTime = -1;

    private LogCheckPointEvent logCheckPointEvent = new LogCheckPointEvent()
    {
        @Override
        public void close()
        {
            endTime = clock.currentTimeMillis();
            totalTime = endTime - startTime;
        }

        @Override
        public LogForceWaitEvent beginLogForceWait()
        {

            return LogForceWaitEvent.NULL;
        }

        @Override
        public LogForceEvent beginLogForce()
        {
            return LogForceEvent.NULL;
        }
    };

    public DefaultCheckPointerTracer()
    {
        this( Clock.SYSTEM_CLOCK );
    }

    DefaultCheckPointerTracer( Clock clock )
    {
        this.clock = clock;
    }

    @Override
    public LogCheckPointEvent beginCheckPoint()
    {
        startTime = clock.currentTimeMillis();
        return logCheckPointEvent;
    }

    @Override
    public long lastCheckPointStartTime()
    {
        return startTime;
    }

    @Override
    public long lastCheckPointEndTime()
    {
        return endTime;
    }

    @Override
    public long lastCheckPointTotalTime()
    {
        return totalTime;
    }
}
