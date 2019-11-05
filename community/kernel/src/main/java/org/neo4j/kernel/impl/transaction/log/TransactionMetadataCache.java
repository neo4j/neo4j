/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.internal.helpers.collection.LruCache;

public class TransactionMetadataCache
{
    private static final int DEFAULT_TRANSACTION_CACHE_SIZE = 100_000;
    private final LruCache<Long /*tx id*/,TransactionMetadata> txStartPositionCache;

    public TransactionMetadataCache()
    {
        this( DEFAULT_TRANSACTION_CACHE_SIZE );
    }

    public TransactionMetadataCache( int transactionCacheSize )
    {
        this.txStartPositionCache = new LruCache<>( "Tx start position cache", transactionCacheSize );
    }

    public void clear()
    {
        txStartPositionCache.clear();
    }

    public TransactionMetadata getTransactionMetadata( long txId )
    {
        return txStartPositionCache.get( txId );
    }

    public void cacheTransactionMetadata( long txId, LogPosition position, int checksum, long timeWritten )
    {
        if ( position.getByteOffset() == -1 )
        {
            throw new RuntimeException( "StartEntry.position is " + position );
        }

        TransactionMetadata result = new TransactionMetadata( position, checksum, timeWritten );
        txStartPositionCache.put( txId, result );
    }

    public static class TransactionMetadata
    {
        private final LogPosition startPosition;
        private final int checksum;
        private final long timeWritten;

        public TransactionMetadata( LogPosition startPosition, int checksum, long timeWritten )
        {
            this.startPosition = startPosition;
            this.checksum = checksum;
            this.timeWritten = timeWritten;
        }

        public LogPosition getStartPosition()
        {
            return startPosition;
        }

        public int getChecksum()
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
            return checksum == that.checksum &&
                   timeWritten == that.timeWritten &&
                   Objects.equals( startPosition, that.startPosition );
        }

        @Override
        public int hashCode()
        {
            int result = startPosition.hashCode();
            result = 31 * result + checksum;
            result = 31 * result + (int) (timeWritten ^ (timeWritten >>> 32));
            return result;
        }
    }
}
