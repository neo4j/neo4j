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
package org.neo4j.ext.udc.impl;

import java.util.Timer;

import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.StartupStatistics;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.udc.UsageData;

/**
 * Kernel extension for UDC, the Usage Data Collector. The UDC runs as a background
 * daemon, waking up once a day to collect basic usage information about a long
 * running graph database.
 * <p>
 * The first update is delayed to avoid needless activity during integration
 * testing and short-run applications. Subsequent updates are made at regular
 * intervals. Both times are specified in milliseconds.
 */
public class UdcKernelExtension implements Lifecycle
{
    private Timer timer;
    private IdGeneratorFactory idGeneratorFactory;
    private StartupStatistics startupStats;
    private final UsageData usageData;
    private final Config config;
    private final DataSourceManager xadsm;

    public UdcKernelExtension( Config config, DataSourceManager xadsm, IdGeneratorFactory idGeneratorFactory,
            StartupStatistics startupStats, UsageData usageData, Timer timer )
    {
        this.config = config;
        this.xadsm = xadsm;
        this.idGeneratorFactory = idGeneratorFactory;
        this.startupStats = startupStats;
        this.usageData = usageData;
        this.timer = timer;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        if ( !config.get( UdcSettings.udc_enabled ) )
        {
            return;
        }

        int firstDelay = config.get( UdcSettings.first_delay );
        int interval = config.get( UdcSettings.interval );
        HostnamePort hostAddress = config.get(UdcSettings.udc_host);

        UdcInformationCollector collector = new DefaultUdcInformationCollector( config, xadsm, idGeneratorFactory,
                startupStats, usageData );
        UdcTimerTask task = new UdcTimerTask( hostAddress, collector );

        timer.scheduleAtFixedRate( task, firstDelay, interval );
    }

    @Override
    public void stop() throws Throwable
    {
        if ( timer != null )
        {
            timer.cancel();
            timer = null;
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
    }
}
