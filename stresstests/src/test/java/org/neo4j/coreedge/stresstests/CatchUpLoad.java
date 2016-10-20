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
package org.neo4j.coreedge.stresstests;

import io.netty.channel.Channel;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.CatchUpClient;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.ClusterMember;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.discovery.EdgeClusterMember;
import org.neo4j.coreedge.handlers.ExceptionMonitoringHandler;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.function.Predicates.await;

class CatchUpLoad extends RepeatUntilCallable
{
    private final Predicate<Throwable> isStoreClosed = new IsStoreClosed();
    private Cluster cluster;

    CatchUpLoad( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster )
    {
        super( keepGoing, onFailure );
        this.cluster = cluster;
    }

    @Override
    protected void doWork()
    {
        CoreClusterMember leader;
        try
        {
            leader = cluster.awaitLeader();
        }
        catch ( TimeoutException e )
        {
            // whatever... we'll try again
            return;
        }

        long txIdBeforeStartingNewEdge = txId( leader, false );
        if ( txIdBeforeStartingNewEdge < TransactionIdStore.BASE_TX_ID )
        {
            // leader has been shut down, let's try again later
            return;
        }
        int newMemberId = cluster.edgeMembers().size();
        final EdgeClusterMember edgeClusterMember = cluster.addEdgeMemberWithId( newMemberId );

        Throwable ex = null;
        Supplier<Throwable> monitoredException = null;
        try
        {
            monitoredException = startAndRegisterExceptionMonitor( edgeClusterMember );
            await( () -> txIdBeforeStartingNewEdge <= txId( edgeClusterMember, true ), 10, TimeUnit.MINUTES );
        }
        catch ( Throwable e )
        {
            ex = e;
        }
        finally
        {
            try
            {
                cluster.removeEdgeMemberWithMemberId( newMemberId );
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

    private Supplier<Throwable> startAndRegisterExceptionMonitor( EdgeClusterMember edgeClusterMember )
    {
        edgeClusterMember.start();

        // the database is create when starting the edge...
        final Monitors monitors =
                edgeClusterMember.database().getDependencyResolver().resolveDependency( Monitors.class );
        ExceptionMonitor exceptionMonitor = new ExceptionMonitor( new IsConnectionRestByPeer() );
        monitors.addMonitorListener( exceptionMonitor, CatchUpClient.class.getName() );
        return exceptionMonitor;
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
