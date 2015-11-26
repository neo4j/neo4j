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

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database Transaction Metrics" )
public class TransactionMetrics extends LifecycleAdapter
{
    private static final String TRANSACTION_PREFIX = "neo4j.transaction";

    @Documented( "The total number of started transactions" )
    public static final String TX_STARTED = name( TRANSACTION_PREFIX, "started" );
    @Documented( "The highest peak of concurrent transactions ever seen on this machine" )
    public static final String TX_PEAK_CONCURRENT = name( TRANSACTION_PREFIX, "peak_concurrent" );

    @Documented( "The number of currently active transactions" )
    public static final String TX_ACTIVE = name( TRANSACTION_PREFIX, "active" );
    @Documented( "The number of currently active read transactions" )
    public static final String READ_TX_ACTIVE = name( TRANSACTION_PREFIX, "active_read" );
    @Documented( "The number of currently active write transactions" )
    public static final String WRITE_TX_ACTIVE = name( TRANSACTION_PREFIX, "active_write" );

    @Documented( "The total number of committed transactions" )
    public static final String TX_COMMITTED = name( TRANSACTION_PREFIX, "committed" );
    @Documented( "The total number of committed read transactions" )
    public static final String READ_TX_COMMITTED = name( TRANSACTION_PREFIX, "committed_read" );
    @Documented( "The total number of committed write transactions" )
    public static final String WRITE_TX_COMMITTED = name( TRANSACTION_PREFIX, "committed_write" );

    @Documented( "The total number of rolled back transactions" )
    public static final String TX_ROLLBACKS = name( TRANSACTION_PREFIX, "rollbacks" );
    @Documented( "The total number of rolled back read transactions" )
    public static final String READ_TX_ROLLBACKS = name( TRANSACTION_PREFIX, "rollbacks_read" );
    @Documented( "The total number of rolled back write transactions" )
    public static final String WRITE_TX_ROLLBACKS = name( TRANSACTION_PREFIX, "rollbacks_write" );

    @Documented( "The total number of terminated transactions" )
    public static final String TX_TERMINATED = name( TRANSACTION_PREFIX, "terminated" );
    @Documented( "The total number of terminated read transactions" )
    public static final String READ_TX_TERMINATED = name( TRANSACTION_PREFIX, "terminated_read" );
    @Documented( "The total number of terminated write transactions" )
    public static final String WRITE_TX_TERMINATED = name( TRANSACTION_PREFIX, "terminated_write" );

    @Documented( "The ID of the last committed transaction" )
    public static final String LAST_COMMITTED_TX_ID = name( TRANSACTION_PREFIX, "last_committed_tx_id" );
    @Documented( "The ID of the last closed transaction" )
    public static final String LAST_CLOSED_TX_ID = name( TRANSACTION_PREFIX, "last_closed_tx_id" );

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

            registry.register( TX_ACTIVE, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfActiveTransactions();
                }
            } );

            registry.register( READ_TX_ACTIVE, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfActiveReadTransactions();
                }
            } );

            registry.register( WRITE_TX_ACTIVE, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfActiveWriteTransactions();
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

            registry.register( READ_TX_COMMITTED, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfCommittedReadTransactions();
                }
            } );

            registry.register( WRITE_TX_COMMITTED, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfCommittedWriteTransactions();
                }
            } );

            registry.register( TX_ROLLBACKS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfRolledBackTransactions();
                }
            } );

            registry.register( READ_TX_ROLLBACKS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfRolledBackReadTransactions();
                }
            } );

            registry.register( WRITE_TX_ROLLBACKS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfRolledBackWriteTransactions();
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

            registry.register( READ_TX_TERMINATED, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfTerminatedReadTransactions();
                }
            } );

            registry.register( WRITE_TX_TERMINATED, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return transactionCounters.getNumberOfTerminatedWriteTransactions();
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
            registry.remove( TX_STARTED );
            registry.remove( TX_PEAK_CONCURRENT );

            registry.remove( TX_ACTIVE );
            registry.remove( READ_TX_ACTIVE );
            registry.remove( WRITE_TX_ACTIVE );

            registry.remove( TX_COMMITTED );
            registry.remove( READ_TX_COMMITTED );
            registry.remove( WRITE_TX_COMMITTED );

            registry.remove( TX_ROLLBACKS );
            registry.remove( READ_TX_ROLLBACKS );
            registry.remove( WRITE_TX_ROLLBACKS );

            registry.remove( TX_TERMINATED );
            registry.remove( READ_TX_TERMINATED );
            registry.remove( WRITE_TX_TERMINATED );

            registry.remove( LAST_COMMITTED_TX_ID );
            registry.remove( LAST_CLOSED_TX_ID );
        }
    }
}
