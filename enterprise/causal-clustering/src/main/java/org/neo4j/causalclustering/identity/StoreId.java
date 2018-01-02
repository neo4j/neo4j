/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import static java.lang.String.format;

import java.util.Objects;

public final class StoreId
{
    public static final StoreId DEFAULT = new StoreId(
            org.neo4j.kernel.impl.store.StoreId.DEFAULT.getCreationTime(),
            org.neo4j.kernel.impl.store.StoreId.DEFAULT.getRandomId(),
            org.neo4j.kernel.impl.store.StoreId.DEFAULT.getUpgradeTime(),
            org.neo4j.kernel.impl.store.StoreId.DEFAULT.getUpgradeId() );

    public static boolean isDefault( StoreId storeId )
    {
        return storeId.getCreationTime() == DEFAULT.getCreationTime() &&
                storeId.getRandomId() == DEFAULT.getRandomId() &&
                storeId.getUpgradeTime() == DEFAULT.getUpgradeTime() &&
                storeId.getUpgradeId() == DEFAULT.getUpgradeId();
    }

    private long creationTime;
    private long randomId;
    private long upgradeTime;
    private long upgradeId;

    public StoreId( long creationTime, long randomId, long upgradeTime, long upgradeId )
    {
        this.creationTime = creationTime;
        this.randomId = randomId;
        this.upgradeTime = upgradeTime;
        this.upgradeId = upgradeId;
    }

    public long getCreationTime()
    {
        return creationTime;
    }

    public long getRandomId()
    {
        return randomId;
    }

    public long getUpgradeTime()
    {
        return upgradeTime;
    }

    public long getUpgradeId()
    {
        return upgradeId;
    }

    public boolean equalToKernelStoreId( org.neo4j.kernel.impl.store.StoreId kenelStoreId )
    {
        return creationTime == kenelStoreId.getCreationTime() &&
               randomId == kenelStoreId.getRandomId() &&
               upgradeTime == kenelStoreId.getUpgradeTime() &&
               upgradeId == kenelStoreId.getUpgradeId();
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
                randomId == storeId.randomId &&
                upgradeTime == storeId.upgradeTime &&
                upgradeId == storeId.upgradeId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( creationTime, randomId, upgradeTime, upgradeId );
    }

    @Override
    public String toString()
    {
        return format( "Store{creationTime:%d, randomId:%s, upgradeTime:%d, upgradeId:%d}",
                creationTime, randomId, upgradeTime, upgradeId );
    }
}
