/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.stresstests;

import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.stresstests.LagEvaluator.Lag;
import org.neo4j.helper.Workload;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.function.Predicates.awaitForever;
import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.ONLY;

class CatchupNewReadReplica extends Workload
{
    private static final long SAMPLE_INTERVAL_MS = 2000;
    private static final long MAX_LAG_MS = 500;

    private final FileSystemAbstraction fs;
    private final Log log;
    private final Cluster<?> cluster;
    private boolean deleteStore;

    CatchupNewReadReplica( Control control, Resources resources )
    {
        super( control );
        this.fs = resources.fileSystem();
        this.cluster = resources.cluster();
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    protected void doWork() throws IOException
    {
        int newMemberId = cluster.readReplicas().size();
        final ReadReplica readReplica = cluster.addReadReplicaWithId( newMemberId );

        log.info( "Adding " + readReplica );
        readReplica.start();

        LagEvaluator lagEvaluator = new LagEvaluator( this::leaderTxId, () -> txId( readReplica ), Clock.systemUTC() );

        awaitForever( () ->
        {
            if ( !control.keepGoing() )
            {
                return true;
            }

            Optional<Lag> lagEstimate = lagEvaluator.evaluate();

            if ( lagEstimate.isPresent() )
            {
                log.info( lagEstimate.get().toString() );
                return lagEstimate.get().timeLagMillis() < MAX_LAG_MS;
            }
            else
            {
                log.info( "Lag estimate not available" );
                return false;
            }
        }, SAMPLE_INTERVAL_MS, MILLISECONDS );

        if ( !control.keepGoing() )
        {
            return;
        }

        log.info( "Caught up" );
        cluster.removeReadReplicaWithMemberId( newMemberId );

        if ( deleteStore )
        {
            log.info( "Deleting store of " + readReplica );
            fs.deleteRecursively( readReplica.databaseDirectory() );
        }
        deleteStore = !deleteStore;
    }

    private OptionalLong leaderTxId()
    {
        try
        {
            return txId( cluster.awaitLeader() );
        }
        catch ( TimeoutException e )
        {
            return OptionalLong.empty();
        }
    }

    private OptionalLong txId( ClusterMember member )
    {
        try
        {
            GraphDatabaseAPI database = member.database();
            TransactionIdStore txIdStore = database.getDependencyResolver().resolveDependency( TransactionIdStore.class, ONLY );
            return OptionalLong.of( txIdStore.getLastClosedTransactionId() );
        }
        catch ( Throwable ex )
        {
            return OptionalLong.empty();
        }
    }
}
