/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class MonitorGc implements Lifecycle
{
    public static class Configuration
    {
        public static final GraphDatabaseSetting<Long> gc_monitor_wait_time = new GraphDatabaseSetting.TimeSpanSetting( "gc_monitor_wait_time" );
        public static final GraphDatabaseSetting<Long> gc_monitor_threshold = new GraphDatabaseSetting.TimeSpanSetting( "gc_monitor_threshold" );
    }
    
    private final Config config;
    private final StringLogger logger;
    private volatile MeasureDoNothing monitorGc;
    
    public MonitorGc( Config config, StringLogger logger )
    {
        this.config = config;
        this.logger = logger;
    }
    
    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        monitorGc = new MeasureDoNothing( "GC-Monitor", logger, config.get( Configuration.gc_monitor_wait_time ), config.get( Configuration.gc_monitor_threshold ) );
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
        // TODO Auto-generated method stub
        
    }

}
