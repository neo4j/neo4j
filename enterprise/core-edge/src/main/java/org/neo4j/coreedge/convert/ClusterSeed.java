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
package org.neo4j.coreedge.convert;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;

import org.neo4j.coreedge.identity.StoreId;

public class ClusterSeed
{
    private final StoreId before;
    private final StoreId after;
    private final long lastTxId;

    public ClusterSeed( StoreId before, StoreId after, long lastTxId )
    {
        this.before = before;
        this.after = after;
        this.lastTxId = lastTxId;
    }

    public static ClusterSeed create( String rawConversionId )
    {
        byte[] bytes = Base64.getDecoder().decode( rawConversionId );
        ByteBuffer buffer = ByteBuffer.wrap( bytes );

        long txId = buffer.getLong();
        StoreId before = readStoreId( buffer );
        StoreId after = readStoreId( buffer );

        return new ClusterSeed( before, after, txId );
    }

    private static StoreId readStoreId( ByteBuffer buffer )
    {
        long creationTime = buffer.getLong();
        long randomId = buffer.getLong();
        long upgradeTime = buffer.getLong();
        long upgradeId = buffer.getLong();

        return new StoreId( creationTime, randomId, upgradeTime, upgradeId );
    }

    public StoreId after()
    {
        return after;
    }

    public String getConversionId()
    {
        int bytesNeeded = 88;
        ByteBuffer buffer = ByteBuffer.allocate( bytesNeeded );

        buffer.putLong( lastTxId );

        buffer.putLong( before.getCreationTime() );
        buffer.putLong( before.getRandomId() );
        buffer.putLong( before.getUpgradeTime() );
        buffer.putLong( before.getUpgradeId() );

        buffer.putLong( after.getCreationTime() );
        buffer.putLong( after.getRandomId() );
        buffer.putLong( after.getUpgradeTime() );
        buffer.putLong( after.getUpgradeId() );

        return Base64.getEncoder().encodeToString( buffer.array() );
    }

    @Override
    public String toString()
    {
        return String.format( "SourceMetadata{before=%s, after=%s, lastTxId=%d}", before, after, lastTxId );
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
        ClusterSeed that = (ClusterSeed) o;
        return lastTxId == that.lastTxId &&
                Objects.equals( before, that.before ) &&
                Objects.equals( after, that.after );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( before, after, lastTxId );
    }

    public StoreId before()
    {
        return before;
    }

    public long lastTxId()
    {
        return lastTxId;
    }
}
