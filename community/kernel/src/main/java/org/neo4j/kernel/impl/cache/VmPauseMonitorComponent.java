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
package org.neo4j.kernel.impl.cache;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.VmPauseMonitor;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

public class VmPauseMonitorComponent implements Lifecycle
{
    private final Config config;
    private final Log log;
    private final JobScheduler jobScheduler;
    private volatile VmPauseMonitor vmPauseMonitor;

    public VmPauseMonitorComponent( Config config, Log log, JobScheduler jobScheduler )
    {
        this.config = config;
        this.log = log;
        this.jobScheduler = jobScheduler;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        vmPauseMonitor = new VmPauseMonitor(
                config.get( GraphDatabaseSettings.vm_pause_monitor_measurement_duration ),
                config.get( GraphDatabaseSettings.vm_pause_monitor_stall_alert_threshold ),
                log, jobScheduler, vmPauseInfo -> log.warn( "Detected VM stop-the-world pause: %s", vmPauseInfo )
        );
        vmPauseMonitor.start();
    }

    @Override
    public void stop()
    {
        vmPauseMonitor.stop();
        vmPauseMonitor = null;
    }

    @Override
    public void shutdown()
    {
    }

}
