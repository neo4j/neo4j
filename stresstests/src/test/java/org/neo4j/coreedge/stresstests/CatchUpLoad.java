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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.neo4j.coreedge.catchup.CatchUpClient;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.ClusterMember;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.discovery.EdgeClusterMember;
import org.neo4j.coreedge.handlers.ExceptionMonitoringHandler;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.function.Predicates.await;

class CatchUpLoad extends RepeatUntilCallable
{
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

        long txIdBeforeStartingNewEdge = txId( leader );
        int newMemberId = cluster.edgeMembers().size();
        final EdgeClusterMember edgeClusterMember = cluster.addEdgeMemberWithId( newMemberId );

        AtomicReference<Throwable> exception;
        try
        {
            exception = startAndRegisterExceptionMonitor( edgeClusterMember );
            await( () -> txIdBeforeStartingNewEdge <= txId( edgeClusterMember ), 3, TimeUnit.MINUTES );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            cluster.removeEdgeMemberWithMemberId( newMemberId );
        }

        if ( exception.get() != null )
        {
            throw new RuntimeException( exception.get() );
        }
    }

    private AtomicReference<Throwable> startAndRegisterExceptionMonitor( EdgeClusterMember edgeClusterMember )
    {
        edgeClusterMember.start();

        // the database is create when starting the edge...
        final Monitors monitors =
                edgeClusterMember.database().getDependencyResolver().resolveDependency( Monitors.class );
        AtomicReference<Throwable> exception = new AtomicReference<>();
        monitors.addMonitorListener( new ExceptionMonitoringHandler.Monitor()
        {
            @Override
            public void exceptionCaught( Channel channel, Throwable cause )
            {
                exception.set( cause );
            }
        }, CatchUpClient.class.getName() );
        return exception;
    }

    private long txId( ClusterMember leader )
    {
        return leader.database().getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastClosedTransactionId();
    }
}
