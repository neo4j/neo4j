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

public class TransactionMetrics extends LifecycleAdapter
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

    private final MetricRegistry registry;
    private final Config config;
    private final DataSourceManager dataSourceManager;
    private final TransactionCounters transactionCounters;

    public TransactionMetrics( MetricRegistry registry, Config config, DataSourceManager dataSourceManager,
            TransactionCounters transactionCounters )

    {
        this.registry = registry;
        this.config = config;

        this.dataSourceManager = dataSourceManager;
        this.transactionCounters = transactionCounters;
    }

    @Override
    public void start() throws Throwable
    {
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
    }

    @Override
    public void stop() throws IOException
    {
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
    }
}
