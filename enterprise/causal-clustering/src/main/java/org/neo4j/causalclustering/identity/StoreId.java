/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.identity;

import java.util.Objects;

import static java.lang.String.format;

public final class StoreId
{
    public static final StoreId DEFAULT = new StoreId(
            org.neo4j.kernel.impl.store.StoreId.DEFAULT.getCreationTime(),
            org.neo4j.kernel.impl.store.StoreId.DEFAULT.getRandomId() );

    public static boolean isDefault( StoreId storeId )
    {
        return storeId.getCreationTime() == DEFAULT.getCreationTime() && storeId.getRandomId() == DEFAULT.getRandomId();
    }

    private long creationTime;
    private long randomId;

    public StoreId( long creationTime, long randomId )
    {
        this.creationTime = creationTime;
        this.randomId = randomId;
    }

    public long getCreationTime()
    {
        return creationTime;
    }

    public long getRandomId()
    {
        return randomId;
    }

    public boolean equalToKernelStoreId( org.neo4j.kernel.impl.store.StoreId kenelStoreId )
    {
        return creationTime == kenelStoreId.getCreationTime() &&
               randomId == kenelStoreId.getRandomId();
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
        if ( isDefault( this ) )
        {
            return false;
        }
        StoreId storeId = (StoreId) o;
        if ( isDefault( storeId ) )
        {
            return false;
        }
        return creationTime == storeId.creationTime &&
                randomId == storeId.randomId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( creationTime, randomId );
    }

    @Override
    public String toString()
    {
        return format( "Store{creationTime:%d, randomId:%s}", creationTime, randomId );
    }
}
