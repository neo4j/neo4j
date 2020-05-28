/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.cache;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.LoggingVmPauseMonitor;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.VmPauseMonitor;
import org.neo4j.scheduler.JobScheduler;

public class VmPauseMonitorComponent extends LifecycleAdapter
{
    private final Config config;
    private final Log log;
    private final JobScheduler jobScheduler;
    private final VmPauseMonitor.Monitor monitor;
    private final LoggingVmPauseMonitor loggingVmPauseMonitor;
    private final Monitors globalMonitors;
    private volatile VmPauseMonitor vmPauseMonitor;

    public VmPauseMonitorComponent( Config config, Log log, JobScheduler jobScheduler, Monitors globalMonitors )
    {
        this.config = config;
        this.log = log;
        this.jobScheduler = jobScheduler;
        this.globalMonitors = globalMonitors;
        monitor = globalMonitors.newMonitor( VmPauseMonitor.Monitor.class );
        loggingVmPauseMonitor = new LoggingVmPauseMonitor( log );
    }

    @Override
    public void start()
    {
        globalMonitors.addMonitorListener( loggingVmPauseMonitor );
        vmPauseMonitor = new VmPauseMonitor(
                config.get( GraphDatabaseInternalSettings.vm_pause_monitor_measurement_duration ),
                config.get( GraphDatabaseInternalSettings.vm_pause_monitor_stall_alert_threshold ),
                monitor, jobScheduler
        );
        vmPauseMonitor.start();
    }

    @Override
    public void stop()
    {
        vmPauseMonitor.stop();
        vmPauseMonitor = null;
        globalMonitors.removeMonitorListener( loggingVmPauseMonitor );
    }
}
