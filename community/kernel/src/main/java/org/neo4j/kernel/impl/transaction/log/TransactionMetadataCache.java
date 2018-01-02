/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.util.Objects;

import org.neo4j.kernel.impl.cache.LruCache;

public class TransactionMetadataCache
{
    private final LruCache<Long /*tx id*/, TransactionMetadata> txStartPositionCache;
    private final LruCache<Long /*log version*/, Long /*last committed tx*/> logHeaderCache;

    public TransactionMetadataCache( int headerCacheSize, int transactionCacheSize )
    {
        this.logHeaderCache = new LruCache<>( "Log header cache", headerCacheSize );
        this.txStartPositionCache = new LruCache<>( "Tx start position cache", transactionCacheSize );
    }

    public void clear()
    {
        logHeaderCache.clear();
        txStartPositionCache.clear();
    }

    public void putHeader( long logVersion, long previousLogLastCommittedTx )
    {
        logHeaderCache.put( logVersion, previousLogLastCommittedTx );
    }

    public long getLogHeader( long logVersion )
    {
        Long value = logHeaderCache.get( logVersion );
        return value == null ? -1 : value;
    }

    public TransactionMetadata getTransactionMetadata( long txId )
    {
        return txStartPositionCache.get( txId );
    }

    public TransactionMetadata cacheTransactionMetadata( long txId, LogPosition position, int masterId,
                                                         int authorId, long checksum, long timeWritten )
    {
        if ( position.getByteOffset() == -1 )
        {
            throw new RuntimeException( "StartEntry.position is " + position );
        }

        TransactionMetadata result = new TransactionMetadata( masterId, authorId, position, checksum, timeWritten );
        txStartPositionCache.put( txId, result );
        return result;
    }

    public static class TransactionMetadata
    {
        private final int masterId;
        private final int authorId;
        private final LogPosition startPosition;
        private final long checksum;
        private final long timeWritten;

        public TransactionMetadata( int masterId, int authorId, LogPosition startPosition, long checksum,
                long timeWritten )
        {
            this.masterId = masterId;
            this.authorId = authorId;
            this.startPosition = startPosition;
            this.checksum = checksum;
            this.timeWritten = timeWritten;
        }

        public int getMasterId()
        {
            return masterId;
        }

        public int getAuthorId()
        {
            return authorId;
        }

        public LogPosition getStartPosition()
        {
            return startPosition;
        }

        public long getChecksum()
        {
            return checksum;
        }

        public long getTimeWritten()
        {
            return timeWritten;
        }

        @Override
        public String toString()
        {
            return "TransactionMetadata{" +
                   "masterId=" + masterId +
                   ", authorId=" + authorId +
                   ", startPosition=" + startPosition +
                   ", checksum=" + checksum +
                   ", timeWritten=" + timeWritten +
                   '}';
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            TransactionMetadata that = (TransactionMetadata) o;
            return masterId == that.masterId &&
                   authorId == that.authorId &&
                   checksum == that.checksum &&
                   timeWritten == that.timeWritten &&
                   Objects.equals( startPosition, that.startPosition );
        }

        @Override
        public int hashCode()
        {
            int result = masterId;
            result = 31 * result + authorId;
            result = 31 * result + startPosition.hashCode();
            result = 31 * result + (int) (checksum ^ (checksum >>> 32));
            result = 31 * result + (int) (timeWritten ^ (timeWritten >>> 32));
            return result;
        }
    }
}
