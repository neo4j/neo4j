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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.IntConsumer;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.concurrent.Scheduler;
import org.neo4j.concurrent.Work;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlavePriority;
import org.neo4j.kernel.ha.com.master.Slaves;
import org.neo4j.kernel.impl.util.CappedLogger;

class PropagateUpdatesWork implements Work<Slaves,PropagateUpdatesWork>
{
    private static class ReplicationContext
    {
        final Future<Response<Void>> future;
        final Slave slave;

        Throwable throwable;

        ReplicationContext( Future<Response<Void>> future, Slave slave )
        {
            this.future = future;
            this.slave = slave;
        }
    }

    private final long txId;
    private int[] authors;
    private final int myAuthorId;
    private final int desiredReplicationFactor;
    private final SlavePriority replicationStrategy;
    private final CappedLogger pushedToTooFewSlaveLogger;
    private final CappedLogger slaveCommitFailureLogger;
    private final IntConsumer missedReplicas;

    public PropagateUpdatesWork( long txId, int authorId, int myAuthorId, int desiredReplicationFactor,
                                 SlavePriority replicationStrategy, CappedLogger pushedToTooFewSlaveLogger,
                                 CappedLogger slaveCommitFailureLogger,
                                 IntConsumer missedReplicas )
    {
        this.txId = txId;
        this.authors = new int[]{authorId};
        this.myAuthorId = myAuthorId;
        this.desiredReplicationFactor = desiredReplicationFactor;
        this.replicationStrategy = replicationStrategy;
        this.pushedToTooFewSlaveLogger = pushedToTooFewSlaveLogger;
        this.slaveCommitFailureLogger = slaveCommitFailureLogger;
        this.missedReplicas = missedReplicas;
    }

    @Override
    public PropagateUpdatesWork combine( PropagateUpdatesWork work )
    {
        // Pick whichever work instance has the "greater" txId, taking the unlikely overflow into account.
        PropagateUpdatesWork combinedWork = txId - work.txId > 0 ? this : work;
        int[] theirAuthors = work.authors;
        int[] ourAuthors = this.authors;
        if ( theirAuthors.length != 1 || ourAuthors.length != 1 || theirAuthors[0] != ourAuthors[0] )
        {
            combinedWork.authors = mergeSortedAuthorSets( theirAuthors, ourAuthors );
        }
        return combinedWork;
    }

    private int[] mergeSortedAuthorSets( int[] theirAuthors, int[] ourAuthors )
    {
        // Since all author lists start out with exactly one element, they are all trivially sorted from the start.
        // We just maintain this initial sorted state throughout our merging.

        // Skip the whole merge thing if the two sets are equal:
        if ( Arrays.equals( theirAuthors, ourAuthors ) )
        {
            return ourAuthors;
        }

        // The two sets are different, so we have to merge them.
        int[] tmp = new int[theirAuthors.length + ourAuthors.length];
        int len = 0;
        for ( int i = 0, j = 0; i < ourAuthors.length && j < theirAuthors.length; )
        {
            int x = ourAuthors[i];
            int y = theirAuthors[j];
            if ( x == y )
            {
                tmp[len] = x;
                i++;
                j++;
            }
            else if ( x < y )
            {
                tmp[len] = x;
                i++;
            }
            else
            {
                tmp[len] = y;
                j++;
            }
            len++;
        }
        return Arrays.copyOf( tmp, len );
    }

    // Note that we are NOT allowing ourselves to throw any checked exceptions!
    @Override
    public void apply( Slaves slaves )
    {
        int replicationFactor = desiredReplicationFactor;
        int successfulReplications = 0;

        // If the author is not this instance, then we need to push to one less - the committer already has it
        if ( authors.length == 1 && authors[0] != myAuthorId )
        {
            replicationFactor--;
        }

        if ( replicationFactor == 0 )
        {
            return;
        }

        Iterator<Slave> slaveList = getPrioritisedSlaveList( slaves );

        // Start as many initial committers as needed
        LinkedList<ReplicationContext> pushFutures = new LinkedList<>();
        for ( int i = 0; i < replicationFactor && slaveList.hasNext(); i++ )
        {
            startParallelPush( slaveList.next(), pushFutures );
        }

        while ( successfulReplications < replicationFactor )
        {
            ReplicationContext response = pushFutures.poll();
            if ( response != null && isSuccessful( response ) )
            {
                successfulReplications++;
            }
            else if ( slaveList.hasNext() )
            {
                startParallelPush( slaveList.next(), pushFutures );
            }
            else
            {
                // We've exhausted the list of slaves to push to.
                // We have to give up reaching our replication factor.
                pushedToTooFewSlaveLogger.info(
                        "Transaction " + txId + " couldn't commit on enough slaves, desired " + replicationFactor +
                        ", but could only commit at " + successfulReplications );

                missedReplicas.accept( replicationFactor - successfulReplications );
                return; // Break out of the while-loop
            }
        }
    }

    private Iterator<Slave> getPrioritisedSlaveList( Slaves slaves )
    {
        Iterator<Slave> slaveList = replicationStrategy.prioritize( slaves.getSlaves() ).iterator();
        if ( authors.length == 1 )
        {
            // Only filter authors if all commits are from a single author.
            slaveList = filter( slaveList, authors[0] );
        }
        return slaveList;
    }

    private static Iterator<Slave> filter( Iterator<Slave> slaves, final Integer externalAuthorServerId )
    {
        return externalAuthorServerId == null ? slaves : new FilteringIterator<>( slaves,
                item -> item.getServerId() != externalAuthorServerId );
    }

    private void startParallelPush( Slave slave, LinkedList<ReplicationContext> pushFutures )
    {
        Callable<Response<Void>> push = () -> slave.pullUpdates( txId );
        Future<Response<Void>> future = Scheduler.executeIOBound( push, Scheduler.OnRejection.CALLER_RUNS );
        pushFutures.offer( new ReplicationContext( future, slave ) );
    }

    private boolean isSuccessful( ReplicationContext context )
    {
        try
        {
            context.future.get();
            return true;
        }
        catch ( InterruptedException e )
        {
            return false;
        }
        catch ( ExecutionException e )
        {
            context.throwable = e.getCause();
            slaveCommitFailureLogger.error(
                    "Slave " + context.slave.getServerId() + ": Replication commit threw" +
                    (context.throwable instanceof ComException ? " communication" : "") + " exception:",
                    context.throwable );
            return false;
        }
        catch ( CancellationException e )
        {
            return false;
        }
    }
}
