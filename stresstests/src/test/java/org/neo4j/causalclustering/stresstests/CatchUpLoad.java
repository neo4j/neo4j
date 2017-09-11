/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.stresstests;

import io.netty.channel.Channel;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.handlers.ExceptionMonitoringHandler;
import org.neo4j.helper.IsConnectionRestByPeer;
import org.neo4j.helper.IsStoreClosed;
import org.neo4j.helper.RepeatUntilCallable;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.function.Predicates.await;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

class CatchUpLoad extends RepeatUntilCallable
{
    private final Predicate<Throwable> isStoreClosed = new IsStoreClosed();
    private final FileSystemAbstraction fs;
    private Cluster cluster;
    private boolean deleteStore;

    CatchUpLoad( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster )
    {
        super( keepGoing, onFailure );
        this.fs = new DefaultFileSystemAbstraction();
        this.cluster = cluster;
    }

    @Override
    protected void doWork()
    {
        int newMemberId = cluster.readReplicas().size();
        final ReadReplica readReplica = cluster.addReadReplicaWithId( newMemberId );

        Throwable ex = null;
        Supplier<Throwable> monitoredException = null;
        try
        {
            monitoredException = startAndRegisterExceptionMonitor( readReplica );
            await( this::leaderTxId, // if the txId from the leader is -1, give up and retry later (leader switch?)
                    leaderTxId -> leaderTxId < BASE_TX_ID || leaderTxId <= txId( readReplica, true ), // caught up?
                    10, TimeUnit.MINUTES );
        }
        catch ( Throwable e )
        {
            ex = e;
        }
        finally
        {
            try
            {
                cluster.removeReadReplicaWithMemberId( newMemberId );
                if ( ex == null && deleteStore )
                {
                    fs.deleteRecursively( readReplica.storeDir() );
                }
                deleteStore = !deleteStore;
            }
            catch ( Throwable e )
            {
                ex = exception( ex, e );
            }
        }

        if ( monitoredException != null && monitoredException.get() != null )
        {
            throw new RuntimeException( exception( monitoredException.get(), ex ) );
        }

        if ( ex != null )
        {
            throw new RuntimeException( ex );
        }
    }

    private Throwable exception( Throwable outer, Throwable inner )
    {
        if ( outer == null )
        {
            assert inner != null;
            return inner;
        }

        if ( inner != null )
        {
            outer.addSuppressed( inner );
        }

        return outer;
    }

    private Supplier<Throwable> startAndRegisterExceptionMonitor( ReadReplica readReplica )
    {
        readReplica.start();

        // the database is create when starting the edge...
        final Monitors monitors =
                readReplica.database().getDependencyResolver().resolveDependency( Monitors.class );
        ExceptionMonitor exceptionMonitor = new ExceptionMonitor( new IsConnectionRestByPeer() );
        monitors.addMonitorListener( exceptionMonitor, CatchUpClient.class.getName() );
        return exceptionMonitor;
    }

    private long leaderTxId()
    {
        try
        {
            return txId( cluster.awaitLeader(), false );
        }
        catch ( TimeoutException e )
        {
            // whatever... we'll try again
            return -1;
        }
    }

    private long txId( ClusterMember member, boolean fail )
    {
        GraphDatabaseAPI database = member.database();
        if ( database == null )
        {
            return errorValueOrThrow( fail, new IllegalStateException( "database is shutdown" ) );
        }

        try
        {
            return database.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                    .getLastClosedTransactionId();
        }
        catch ( Throwable ex )
        {
            return errorValueOrThrow( fail && !isStoreClosed.test( ex ), ex );
        }
    }

    private long errorValueOrThrow( boolean fail, Throwable error )
    {
        if ( fail )
        {
            throw new RuntimeException( error );
        }
        else
        {
            return -1;
        }
    }

    private static class ExceptionMonitor implements ExceptionMonitoringHandler.Monitor, Supplier<Throwable>
    {
        private final AtomicReference<Throwable> exception = new AtomicReference<>();
        private Predicate<Throwable> reject;

        ExceptionMonitor( Predicate<Throwable> reject )
        {
            this.reject = reject;
        }

        @Override
        public void exceptionCaught( Channel channel, Throwable cause )
        {
            if ( !reject.test( cause ) )
            {
                exception.set( cause );
            }
        }

        @Override
        public Throwable get()
        {
            return exception.get();
        }
    }
}
