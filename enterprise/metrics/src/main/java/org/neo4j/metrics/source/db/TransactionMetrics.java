/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.function.Supplier;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database transaction metrics" )
public class TransactionMetrics extends LifecycleAdapter
{
    private static final String TRANSACTION_PREFIX = "neo4j.transaction";

    @Documented( "The total number of started transactions" )
    public static final String TX_STARTED = name( TRANSACTION_PREFIX, "started" );
    @Documented( "The highest peak of concurrent transactions" )
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
    private final TransactionCounters transactionCounters;
    private final Supplier<TransactionIdStore> transactionIdStore;

    public TransactionMetrics( MetricRegistry registry,
            Supplier<TransactionIdStore> transactionIdStore, TransactionCounters transactionCounters )
    {
        this.registry = registry;
        this.transactionIdStore = transactionIdStore;
        this.transactionCounters = transactionCounters;
    }

    @Override
    public void start()
    {
        registry.register( TX_STARTED, (Gauge<Long>) transactionCounters::getNumberOfStartedTransactions );
        registry.register( TX_PEAK_CONCURRENT,
                (Gauge<Long>) transactionCounters::getPeakConcurrentNumberOfTransactions );

        registry.register( TX_ACTIVE, (Gauge<Long>) transactionCounters::getNumberOfActiveTransactions );
        registry.register( READ_TX_ACTIVE, (Gauge<Long>) transactionCounters::getNumberOfActiveReadTransactions );
        registry.register( WRITE_TX_ACTIVE, (Gauge<Long>) transactionCounters::getNumberOfActiveWriteTransactions );

        registry.register( TX_COMMITTED, (Gauge<Long>) transactionCounters::getNumberOfCommittedTransactions );
        registry.register( READ_TX_COMMITTED, (Gauge<Long>) transactionCounters::getNumberOfCommittedReadTransactions );
        registry.register( WRITE_TX_COMMITTED,
                (Gauge<Long>) transactionCounters::getNumberOfCommittedWriteTransactions );

        registry.register( TX_ROLLBACKS, (Gauge<Long>) transactionCounters::getNumberOfRolledBackTransactions );
        registry.register( READ_TX_ROLLBACKS,
                (Gauge<Long>) transactionCounters::getNumberOfRolledBackReadTransactions );
        registry.register( WRITE_TX_ROLLBACKS,
                (Gauge<Long>) transactionCounters::getNumberOfRolledBackWriteTransactions );

        registry.register( TX_TERMINATED, (Gauge<Long>) transactionCounters::getNumberOfTerminatedTransactions );
        registry.register( READ_TX_TERMINATED,
                (Gauge<Long>) transactionCounters::getNumberOfTerminatedReadTransactions );
        registry.register( WRITE_TX_TERMINATED,
                (Gauge<Long>) transactionCounters::getNumberOfTerminatedWriteTransactions );

        registry.register( LAST_COMMITTED_TX_ID, (Gauge<Long>) () ->
                transactionIdStore.get().getLastCommittedTransactionId() );
        registry.register( LAST_CLOSED_TX_ID, (Gauge<Long>) () ->
                transactionIdStore.get().getLastClosedTransactionId() );
    }

    @Override
    public void stop()
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
