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
package org.neo4j.kernel.impl.cache;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.setting;

public class MonitorGc implements Lifecycle
{
    public static class Configuration
    {
        public static final Setting<Long> gc_monitor_wait_time = setting( "gc_monitor_wait_time", DURATION, "100ms" );
        public static final Setting<Long> gc_monitor_threshold = setting("gc_monitor_threshold", DURATION, "200ms" );
    }

    private final Config config;
    private final Log log;
    private volatile MeasureDoNothing monitorGc;

    public MonitorGc( Config config, Log log )
    {
        this.config = config;
        this.log = log;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        monitorGc = new MeasureDoNothing( "neo4j.PauseMonitor", log, config.get( Configuration.gc_monitor_wait_time ),
                config.get( Configuration.gc_monitor_threshold ) );
        monitorGc.start();
    }

    @Override
    public void stop() throws Throwable
    {
        monitorGc.stopMeasuring();
        monitorGc = null;
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

}
