/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api;

import java.security.SecureRandom;
import java.util.Objects;
import java.util.Random;

public final class StoreId
{
    public static final StoreId UNKNOWN = new StoreId( -1, -1, -1, -1, -1 );

    private static final Random r = new SecureRandom();

    private final long creationTime;
    private final long randomId;
    private final long storeVersion;
    private final long upgradeTime;
    private final long upgradeTxId;

    public StoreId( long storeVersion )
    {
        // If creationTime == upgradeTime && randomNumber == upgradeTxId then store has never been upgraded
        long currentTimeMillis = System.currentTimeMillis();
        long randomLong = r.nextLong();
        this.storeVersion = storeVersion;
        this.creationTime = currentTimeMillis;
        this.randomId = randomLong;
        this.upgradeTime = currentTimeMillis;
        this.upgradeTxId = randomLong;
    }

    public StoreId( long creationTime, long randomId, long storeVersion )
    {
        this( creationTime, randomId, storeVersion, creationTime, randomId );
    }

    public StoreId( long creationTime, long randomId, long storeVersion, long upgradeTime, long upgradeTxId )
    {
        this.creationTime = creationTime;
        this.randomId = randomId;
        this.storeVersion = storeVersion;
        this.upgradeTime = upgradeTime;
        this.upgradeTxId = upgradeTxId;
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

    public long getUpgradeTxId()
    {
        return upgradeTxId;
    }

    public long getStoreVersion()
    {
        return storeVersion;
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
        StoreId storeId = (StoreId) o;
        return creationTime == storeId.creationTime &&
               randomId == storeId.randomId &&
               storeVersion == storeId.storeVersion &&
               upgradeTime == storeId.upgradeTime &&
               upgradeTxId == storeId.upgradeTxId;
    }

    public boolean equalsIgnoringUpdate( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        StoreId storeId = (StoreId) o;
        return creationTime == storeId.creationTime && randomId == storeId.randomId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( creationTime, randomId, storeVersion, upgradeTime, upgradeTxId );
    }

    @Override
    public String toString()
    {
        return "StoreId{" +
                "creationTime=" + creationTime +
                ", randomId=" + randomId +
                ", storeVersion=" + storeVersion +
                ", upgradeTime=" + upgradeTime +
                ", upgradeTxId=" + upgradeTxId +
                '}';
    }
}
