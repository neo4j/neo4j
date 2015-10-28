/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.metrics.source;

import com.codahale.metrics.MetricRegistry;

import java.io.IOException;

import org.neo4j.function.Factory;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

public class Neo4jMetricsFactory implements Factory<Lifecycle>
{
    private final LogService logService;
    private final MetricRegistry registry;
    private final Config config;
    private final Monitors monitors;
    private final TransactionCounters transactionCounters;
    private final PageCacheMonitor pageCacheCounters;
    private final CheckPointerMonitor checkPointerMonitor;
    private final IdGeneratorFactory idGeneratorFactory;

    public Neo4jMetricsFactory( LogService logService, MetricRegistry registry, Config config, Monitors monitors,
            TransactionCounters transactionCounters, PageCacheMonitor pageCacheCounters,
            CheckPointerMonitor checkPointerMonitor, IdGeneratorFactory idGeneratorFactory )
    {
        this.logService = logService;
        this.registry = registry;
        this.config = config;
        this.monitors = monitors;
        this.transactionCounters = transactionCounters;
        this.pageCacheCounters = pageCacheCounters;
        this.checkPointerMonitor = checkPointerMonitor;
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    public Lifecycle newInstance()
    {
        final DBMetrics dbMetrics = new DBMetrics( registry, config,
                transactionCounters, pageCacheCounters, checkPointerMonitor, idGeneratorFactory );
        final NetworkMetrics networkMetrics = new NetworkMetrics( config, monitors, registry );
        final JvmMetrics jvmMetrics = new JvmMetrics( logService, config, registry );
        return new LifecycleAdapter()
        {
            @Override
            public void start() throws Throwable
            {
                dbMetrics.start();
                networkMetrics.start();
                jvmMetrics.start();
            }

            @Override
            public void stop() throws IOException
            {
                dbMetrics.stop();
                networkMetrics.stop();
                jvmMetrics.stop();
            }
        };
    }
}
