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

import org.neo4j.kernel.impl.store.StoreId;

public class SourceMetadata
{
    private final StoreId before;
    private final StoreId after;
    private final long lastTxId;

    public SourceMetadata( StoreId before, StoreId after, long lastTxId )
    {
        this.before = before;
        this.after = after;
        this.lastTxId = lastTxId;
    }

    public static SourceMetadata create( String rawConversionId )
    {
        byte[] bytes = Base64.getDecoder().decode( rawConversionId );
        ByteBuffer buffer = ByteBuffer.wrap( bytes );

        long txId = buffer.getLong();
        StoreId before = readStoreId( buffer );
        StoreId after = readStoreId( buffer );

        return new SourceMetadata( before, after, txId );
    }

    private static StoreId readStoreId( ByteBuffer buffer )
    {
        long creationTime = buffer.getLong();
        long randomId = buffer.getLong();
        long storeVersion = buffer.getLong();
        long upgradeTime = buffer.getLong();
        long upgradeId = buffer.getLong();

        return new StoreId( creationTime, randomId, storeVersion, upgradeTime, upgradeId );
    }

    public StoreId after()
    {
        return after;
    }

    public String getConversionId()
    {
        ByteBuffer buffer = ByteBuffer.allocate( 88 );

        buffer.putLong( lastTxId );

        buffer.putLong( before.getCreationTime() );
        buffer.putLong( before.getRandomId() );
        buffer.putLong( before.getStoreVersion() );
        buffer.putLong( before.getUpgradeTime() );
        buffer.putLong( before.getUpgradeId() );

        buffer.putLong( after.getCreationTime() );
        buffer.putLong( after.getRandomId() );
        buffer.putLong( after.getStoreVersion() );
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
        SourceMetadata that = (SourceMetadata) o;
        return lastTxId == that.lastTxId &&
                storeIdEquals( before, that.before ) &&
                storeIdEquals( after, that.after );
    }

    @Override
    public int hashCode()
    {
        int result = 31 + storeIdHashcode( before );
        result = 31 * result + storeIdHashcode( after );
        return 31 * result + Objects.hash( lastTxId );
    }

    private int storeIdHashcode( StoreId storeId )
    {
        return this.before == null ? 0 : storeId.theRealHashCode();
    }

    private boolean storeIdEquals( StoreId one, StoreId two)
    {
        return (one == two || (one != null && one.theRealEquals( two )));
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
