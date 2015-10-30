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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;

import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;

public class DBMetrics extends LifecycleAdapter
{
    private static final String TRANSACTION_PREFIX = "neo4j.transaction";
    private static final String TX_PEAK_CONCURRENT = name( TRANSACTION_PREFIX, "peak_concurrent" );
    private static final String TX_STARTED = name( TRANSACTION_PREFIX, "started" );
    private static final String TX_TERMINATED = name( TRANSACTION_PREFIX, "terminated" );
    private static final String TX_ROLLBACKS = name( TRANSACTION_PREFIX, "rollbacks" );
    private static final String TX_COMMITTED = name( TRANSACTION_PREFIX, "committed" );
    private static final String TX_ACTIVE = name( TRANSACTION_PREFIX, "active" );
    private static final String LAST_COMMITTED_TX_ID = name( TRANSACTION_PREFIX, "last_committed_tx_id" );
    private static final String LAST_CLOSED_TX_ID = name( TRANSACTION_PREFIX, "last_closed_tx_id" );

    private static final String PAGE_CACHE_PREFIX = "neo4j.page_cache";
    private static final String PC_EVICTION_EXCEPTIONS = name( PAGE_CACHE_PREFIX, "eviction_exceptions" );
    private static final String PC_FLUSHES = name( PAGE_CACHE_PREFIX, "flushes" );
    private static final String PC_UNPINS = name( PAGE_CACHE_PREFIX, "unpins" );
    private static final String PC_PINS = name( PAGE_CACHE_PREFIX, "pins" );
    private static final String PC_EVICTIONS = name( PAGE_CACHE_PREFIX, "evictions" );
    private static final String PC_PAGE_FAULTS = name( PAGE_CACHE_PREFIX, "page_faults" );

    private static final String CHECK_POINT_PREFIX = "neo4j.check_point";
    private static final String CHECK_POINT_EVENTS = name( CHECK_POINT_PREFIX, "events" );
    private static final String CHECK_POINT_TOTAL_TIME = name( CHECK_POINT_PREFIX, "total_time" );

    private static final String LOG_ROTATION_PREFIX = "neo4j.log_rotation";
    private static final String LOG_ROTATION_EVENTS = name( LOG_ROTATION_PREFIX, "events" );
    private static final String LOG_ROTATION_TOTAL_TIME = name( LOG_ROTATION_PREFIX, "total_time" );

    private static final String COUNTS_PREFIX = "neo4j.ids_in_use";
    private static final String COUNTS_RELATIONSHIP_TYPE = name( COUNTS_PREFIX, "relationship_type" );
    private static final String COUNTS_PROPERTY = name( COUNTS_PREFIX, "property" );
    private static final String COUNTS_RELATIONSHIP = name( COUNTS_PREFIX, "relationship" );
    private static final String COUNTS_NODE = name( COUNTS_PREFIX, "node" );

    private final MetricRegistry registry;
    private final Config config;
    private final DataSourceManager dataSourceManager;
    private final TransactionCounters transactionCounters;
    private final PageCacheMonitor pageCacheCounters;
    private final CheckPointerMonitor checkPointerMonitor;
    private final LogRotationMonitor logRotationMonitor;
    private final IdGeneratorFactory idGeneratorFactory;

    public DBMetrics( MetricRegistry registry, Config config, DataSourceManager dataSourceManager,
            TransactionCounters transactionCounters, PageCacheMonitor pageCacheCounters,
            CheckPointerMonitor checkPointerMonitor, LogRotationMonitor logRotationMonitor,
            IdGeneratorFactory idGeneratorFactory )
    {
        this.registry = registry;
        this.config = config;

        this.dataSourceManager = dataSourceManager;
        this.transactionCounters = transactionCounters;
        this.pageCacheCounters = pageCacheCounters;
        this.checkPointerMonitor = checkPointerMonitor;
        this.logRotationMonitor = logRotationMonitor;
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    public void start() throws Throwable
    {
        // Neo stats
        // TxManager metrics
        if ( config.get( MetricsSettings.neoTxEnabled ) )
        {
            registry.register( TX_ACTIVE, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfActiveTransactions();
                }
            } );

            registry.register( TX_COMMITTED, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfCommittedTransactions();
                }
            } );

            registry.register( TX_ROLLBACKS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfRolledbackTransactions();
                }
            } );

            registry.register( TX_TERMINATED, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfTerminatedTransactions();
                }
            } );

            registry.register( TX_STARTED, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfStartedTransactions();
                }
            } );

            registry.register( TX_PEAK_CONCURRENT, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getPeakConcurrentNumberOfTransactions();
                }
            } );

            registry.register( LAST_COMMITTED_TX_ID, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return dataSourceManager.getDataSource().
                            getNeoStores().getMetaDataStore().getLastCommittedTransactionId();
                }
            } );

            registry.register( LAST_CLOSED_TX_ID, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return dataSourceManager.getDataSource().
                            getNeoStores().getMetaDataStore().getLastClosedTransactionId();
                }
            } );
        }

        // Page cache metrics
        if ( config.get( MetricsSettings.neoPageCacheEnabled ) )
        {
            registry.register( PC_PAGE_FAULTS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countFaults();
                }
            } );

            registry.register( PC_EVICTIONS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countEvictions();
                }
            } );

            registry.register( PC_PINS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countPins();
                }
            } );

            registry.register( PC_UNPINS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countUnpins();
                }
            } );

            registry.register( PC_FLUSHES, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countFlushes();
                }
            } );

            registry.register( PC_EVICTION_EXCEPTIONS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countEvictionExceptions();
                }
            } );
        }

        // check pointing
        if ( config.get( MetricsSettings.neoCheckPointingEnabled ) )
        {
            registry.register( CHECK_POINT_EVENTS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return checkPointerMonitor.numberOfCheckPointEvents();
                }
            } );

            registry.register( CHECK_POINT_TOTAL_TIME, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return checkPointerMonitor.checkPointAccumulatedTotalTimeMillis();
                }
            } );
        }

        // log rotation
        if ( config.get( MetricsSettings.neoLogRotationEnabled ) )
        {
            registry.register( LOG_ROTATION_EVENTS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return logRotationMonitor.numberOfLogRotationEvents();
                }
            } );

            registry.register( LOG_ROTATION_TOTAL_TIME, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return logRotationMonitor.logRotationAccumulatedTotalTimeMillis();
                }
            } );
        }

        // Node/rel count metrics
        if ( config.get( MetricsSettings.neoCountsEnabled ) )
        {
            registry.register( COUNTS_NODE, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return idGeneratorFactory.get( IdType.NODE ).getNumberOfIdsInUse();
                }
            } );

            registry.register( COUNTS_RELATIONSHIP, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return idGeneratorFactory.get( IdType.RELATIONSHIP ).getNumberOfIdsInUse();
                }
            } );

            registry.register( COUNTS_PROPERTY, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return idGeneratorFactory.get( IdType.PROPERTY ).getNumberOfIdsInUse();
                }
            } );

            registry.register( COUNTS_RELATIONSHIP_TYPE, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return idGeneratorFactory.get( IdType.RELATIONSHIP_TYPE_TOKEN ).getNumberOfIdsInUse();
                }
            } );
        }
    }

    @Override
    public void stop() throws IOException
    {
        // Neo stats
        // TxManager metrics
        if ( config.get( MetricsSettings.neoTxEnabled ) )
        {
            registry.remove( TX_ACTIVE );
            registry.remove( TX_COMMITTED );
            registry.remove( TX_ROLLBACKS );
            registry.remove( TX_TERMINATED );
            registry.remove( TX_STARTED );
            registry.remove( TX_PEAK_CONCURRENT );
            registry.remove( LAST_COMMITTED_TX_ID );
            registry.remove( LAST_CLOSED_TX_ID );
        }

        // Page cache metrics
        if ( config.get( MetricsSettings.neoPageCacheEnabled ) )
        {
            registry.remove( PC_PAGE_FAULTS );
            registry.remove( PC_EVICTIONS );
            registry.remove( PC_PINS );
            registry.remove( PC_UNPINS );
            registry.remove( PC_FLUSHES );
            registry.remove( PC_EVICTION_EXCEPTIONS );
        }

        // check pointing
        if ( config.get( MetricsSettings.neoCheckPointingEnabled ) )
        {
            registry.remove( CHECK_POINT_EVENTS );
            registry.remove( CHECK_POINT_TOTAL_TIME );
        }

        // log rotation
        if ( config.get( MetricsSettings.neoCheckPointingEnabled ) )
        {
            registry.remove( LOG_ROTATION_EVENTS );
            registry.remove( LOG_ROTATION_TOTAL_TIME );
        }

        // Node/rel count metrics
        if ( config.get( MetricsSettings.neoCountsEnabled ) )
        {
            registry.remove( COUNTS_NODE );
            registry.remove( COUNTS_RELATIONSHIP );
            registry.remove( COUNTS_PROPERTY );
            registry.remove( COUNTS_RELATIONSHIP_TYPE );
        }
    }
}
