/*
 * Copyright (c) "Neo4j"
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
    private static final int DEFAULT_TRANSACTION_CACHE_SIZE = 10_000;
    private final LruCache<Long,TransactionMetadata> txIdMetadataCache;

    public TransactionMetadataCache()
    {
        this.txIdMetadataCache = new LruCache<>( "Tx start position cache", DEFAULT_TRANSACTION_CACHE_SIZE );
    }

    public void clear()
    {
        txIdMetadataCache.clear();
    }

    public TransactionMetadata getTransactionMetadata( long txId )
    {
        return txIdMetadataCache.get( txId );
    }

    public void cacheTransactionMetadata( long txId, LogPosition position )
    {
        if ( position.getByteOffset() == -1 )
        {
            throw new RuntimeException( "StartEntry.position is " + position );
        }

        TransactionMetadata result = new TransactionMetadata( position );
        txIdMetadataCache.put( txId, result );
    }

    public static class TransactionMetadata
    {
        private final LogPosition startPosition;

        public TransactionMetadata( LogPosition startPosition )
        {
            this.startPosition = startPosition;
        }

        public LogPosition getStartPosition()
        {
            return startPosition;
        }

        @Override
        public String toString()
        {
            return "TransactionMetadata{" +
                   ", startPosition=" + startPosition +
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
            return Objects.equals( startPosition, that.startPosition );
        }

        @Override
        public int hashCode()
        {
            return startPosition.hashCode();
        }
    }
}
