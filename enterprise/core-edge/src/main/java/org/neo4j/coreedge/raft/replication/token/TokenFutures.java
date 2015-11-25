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
package org.neo4j.coreedge.raft.replication.token;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TokenFutures
{
    private final ReadWriteLock waitListLock = new ReentrantReadWriteLock();
    private final Map<String, ArrayList<CompletableFuture<Integer>>> waitLists = new HashMap<>();

    // TODO: Consider returning a context for more efficiently disposing the future.
    public CompletableFutureTokenId createFuture( final String key )
    {
        CompletableFutureTokenId completableFutureTokenId = new CompletableFutureTokenId()
        {
            @Override
            public void close() throws Exception
            {
                disposeFuture( key, this );
            }
        };

        waitListLock.writeLock().lock();
        try
        {
            ArrayList<CompletableFuture<Integer>> waitList = waitLists.get( key );
            if ( waitList == null )
            {
                waitList = new ArrayList<>();
                waitLists.put( key, waitList );
            }

            waitList.add( completableFutureTokenId );
        }
        finally
        {
            waitListLock.writeLock().unlock();
        }

        return completableFutureTokenId;
    }

    private void disposeFuture( String key, Future<Integer> future )
    {
        waitListLock.writeLock().lock();
        try
        {
            ArrayList<CompletableFuture<Integer>> waitList = waitLists.get( key );

            ListIterator<CompletableFuture<Integer>> waitListIterator = waitList.listIterator();
            while ( waitListIterator.hasNext() )
            {
                if ( waitListIterator.next() == future )
                {
                    waitListIterator.remove();
                    if ( waitList.size() == 0 )
                    {
                        waitLists.remove( key );
                    }
                    return;
                }
            }

            throw new IllegalStateException( "Future to dispose was not found." );
        }
        finally
        {
            waitListLock.writeLock().unlock();
        }
    }

    public void complete( String key, Integer result )
    {
        waitListLock.readLock().lock();
        try
        {
            ArrayList<CompletableFuture<Integer>> waitList = waitLists.get( key );
            if ( waitList == null )
            {
                return;
            }
            for ( CompletableFuture<Integer> future : waitList )
            {
                future.complete( result );
            }
        }
        finally
        {
            waitListLock.readLock().unlock();
        }
    }

    public abstract static class CompletableFutureTokenId extends CompletableFuture<Integer> implements FutureTokenId
    {
    }

    public interface FutureTokenId extends java.lang.AutoCloseable, Future<Integer>
    {
    }


}
