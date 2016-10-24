/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.ha.transaction;

import java.util.concurrent.ExecutionException;
import java.util.function.IntConsumer;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.concurrent.WorkSync;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.com.master.SlavePriorities;
import org.neo4j.kernel.ha.com.master.SlavePriority;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.util.CappedLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Pushes transactions committed on master to one or more slaves. Number of slaves receiving each transactions
 * is controlled by {@link HaSettings#tx_push_factor}. Which slaves receives transactions is controlled by
 * {@link HaSettings#tx_push_strategy}.
 *
 * An attempt is made to push each transaction to the wanted number of slaves, but if it isn't possible
 * and a timeout is hit, propagation will still be considered as successful and occurrence will be logged.
 */
public class TransactionPropagator implements Lifecycle
{
    public interface Configuration
    {
        int getTxPushFactor();

        InstanceId getServerId();

        SlavePriority getReplicationStrategy();
    }

    public static Configuration from( final Config config )
    {
        return new Configuration()
        {
            @Override
            public int getTxPushFactor()
            {
                return config.get( HaSettings.tx_push_factor );
            }

            @Override
            public InstanceId getServerId()
            {
                return config.get( ClusterSettings.server_id );
            }

            @Override
            public SlavePriority getReplicationStrategy()
            {
                switch ( config.get( HaSettings.tx_push_strategy ) )
                {
                    case fixed_descending:
                        return SlavePriorities.fixedDescending();

                    case fixed_ascending:
                        return SlavePriorities.fixedAscending();

                    case round_robin:
                        return SlavePriorities.roundRobin();

                    default:
                        throw new RuntimeException( "Unknown replication strategy " );
                }
            }
        };
    }

    public static Configuration from( final Config config, final SlavePriority slavePriority )
    {
        return new Configuration()
        {
            @Override
            public int getTxPushFactor()
            {
                return config.get( HaSettings.tx_push_factor );
            }

            @Override
            public InstanceId getServerId()
            {
                return config.get( ClusterSettings.server_id );
            }

            @Override
            public SlavePriority getReplicationStrategy()
            {
                return slavePriority;
            }
        };
    }

    private int desiredReplicationFactor;
    private SlavePriority replicationStrategy;
    private final Configuration config;
    private final int myAuthorId;
    private final CappedLogger slaveCommitFailureLogger;
    private final CappedLogger pushedToTooFewSlaveLogger;
    private final WorkSync<Slaves,PropagateUpdatesWork> slaveSync;

    public TransactionPropagator( Configuration config, Log log, Slaves slaves )
    {
        this.config = config;
        myAuthorId = config.getServerId().toIntegerIndex();
        slaveCommitFailureLogger = new CappedLogger( log ).setTimeLimit( 5, SECONDS, Clocks.systemClock() );
        pushedToTooFewSlaveLogger = new CappedLogger( log ).setTimeLimit( 5, SECONDS, Clocks.systemClock() );
        slaveSync = new WorkSync<>( slaves );
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        desiredReplicationFactor = config.getTxPushFactor();
        replicationStrategy = config.getReplicationStrategy();
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
    {
    }

    /**
     * Propagates transactions at least up to and including the given transaction id, which was committed by the given
     * authorId, which is a server id.
     *
     * The number of slaves receiving each transactions is controlled by {@link HaSettings#tx_push_factor}.
     * Which slaves receives transactions is controlled by {@link HaSettings#tx_push_strategy}.
     *
     * We assume that <strong>this</strong> HA instance is the master of the cluster. If the author is not
     * <strong>this</strong> instance, then it was committed on a slave instance, and that instead would then already
     * have the given transaction. Thus, if the author id differs from the server id of this instance, then we will
     * push to one less than push-factor number of slave instances.
     *
     * The pushing to all slaves happen in parallel, but this method as a whole is a blocking operation, and will not
     * return until all designated slaves have received the commit, or they have timed out.
     * @param txId transaction id to replicate
     * @param authorId author id for such transaction id
     * @param missedReplicas callback for the number of missed replicas (e.g., desired replication factor - number of
     * successful replications)
     */
    public void committed( long txId, int authorId, IntConsumer missedReplicas )
    {
        PropagateUpdatesWork work = new PropagateUpdatesWork(
                txId, authorId, myAuthorId, desiredReplicationFactor, replicationStrategy,
                pushedToTooFewSlaveLogger, slaveCommitFailureLogger, missedReplicas );
        try
        {
            slaveSync.apply( work );
        }
        catch ( ExecutionException e )
        {
            throw (RuntimeException) e.getCause();
        }
    }
}
