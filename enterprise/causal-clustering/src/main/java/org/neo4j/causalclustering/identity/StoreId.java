/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
