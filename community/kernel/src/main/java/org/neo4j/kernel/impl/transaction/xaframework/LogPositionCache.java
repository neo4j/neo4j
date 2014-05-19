/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.xaframework;

import org.neo4j.kernel.impl.cache.LruCache;

public class LogPositionCache
{
    private final LruCache<Long, LogPosition> txStartPositionCache;
    private final LruCache<Long /*log version*/, Long /*last committed tx*/> logHeaderCache;

    public LogPositionCache( int headerCacheSize, int transactionCacheSize )
    {
        this.logHeaderCache = new LruCache<>( "Log header cache", headerCacheSize );
        this.txStartPositionCache = new LruCache<>( "Tx start position cache", transactionCacheSize );
    }

    public void clear()
    {
        logHeaderCache.clear();
        txStartPositionCache.clear();
    }

    public LogPosition positionOf( long txId )
    {
        return txStartPositionCache.get( txId );
    }

    public void putHeader( long logVersion, long previousLogLastCommittedTx )
    {
        logHeaderCache.put( logVersion, previousLogLastCommittedTx );
    }

    public Long getHeader( long logVersion )
    {
        return logHeaderCache.get( logVersion );
    }

    public void putStartPosition( long txId, LogPosition position )
    {
        txStartPositionCache.put( txId, position );
    }

    public LogPosition getStartPosition( long txId )
    {
        return txStartPositionCache.get( txId );
    }

//    public synchronized TxPosition cacheStartPosition( long txId, LogEntry.Start startEntry, long logVersion )
//    {
//        if ( startEntry.getStartPosition() == -1 )
//        {
//            throw new RuntimeException( "StartEntry.position is " + startEntry.getStartPosition() );
//        }
//
//        LogPosition result = new TxPosition( logVersion, startEntry.getMasterId(), startEntry.getIdentifier(),
//                startEntry.getStartPosition(), startEntry.getChecksum() );
//        putStartPosition( txId, result );
//        return result;
//    }
}
