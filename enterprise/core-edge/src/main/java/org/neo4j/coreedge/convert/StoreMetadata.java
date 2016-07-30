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

import java.util.Objects;

import org.neo4j.coreedge.identity.StoreId;

public class StoreMetadata
{
    private final StoreId storeId;
    private final long lastTxId;

    public StoreMetadata( StoreId storeId, long lastTxId )
    {
        this.storeId = storeId;
        this.lastTxId = lastTxId;
    }

    @Override
    public String toString()
    {
        return String.format( "TargetMetadata{before=%s, lastTxId=%d}", storeId, lastTxId );
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
        StoreMetadata that = (StoreMetadata) o;
        return lastTxId == that.lastTxId && Objects.equals( storeId, that.storeId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( storeId, lastTxId );
    }

    public StoreId storeId()
    {
        return storeId;
    }

    public long lastTxId()
    {
        return lastTxId;
    }
}
