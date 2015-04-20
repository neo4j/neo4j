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
package org.neo4j.kernel.logging;

import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Allows the roll-over event to be monitored. We need this to be able to output diagnostics at the beginning of every log.
 */
public class MonitoredRollingPolicy
    extends TimeBasedRollingPolicy
{
    private static Monitors monitors;

    public static void setMonitorsInstance(Monitors monitorsInstance)
    {
        monitors = monitorsInstance;
    }

    private RollingLogMonitor monitor;

    public MonitoredRollingPolicy()
    {
        if (monitors != null)
            monitor = monitors.newMonitor(RollingLogMonitor.class);
    }

    @Override
    public void rollover() throws RolloverFailure
    {
        super.rollover();

        monitor.rolledOver();
    }
}
