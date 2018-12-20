/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.time.Clock;

import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;

public class DefaultCheckPointerTracer implements CheckPointTracer
{
    private final Clock clock;
    private final CheckPointerMonitor monitor;
    private volatile long startTimeMillis;

    private LogCheckPointEvent logCheckPointEvent = new LogCheckPointEvent()
    {
        @Override
        public void close()
        {
            notifyMonitor();
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

    public DefaultCheckPointerTracer( Clock clock, CheckPointerMonitor monitor )
    {
        this.clock = clock;
        this.monitor = monitor;
    }

    @Override
    public LogCheckPointEvent beginCheckPoint()
    {
        startTimeMillis = clock.millis();
        return logCheckPointEvent;
    }

    private void notifyMonitor()
    {
        long lastEventTimeMillis = clock.millis() - startTimeMillis;
        monitor.checkPointCompleted( lastEventTimeMillis );
    }
}
