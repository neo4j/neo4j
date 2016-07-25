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
package org.neo4j.coreedge.server;

import java.util.Objects;

public final class StoreId
{

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
        return "Store{" + Long.toString( randomId, Character.MAX_RADIX )+ "_" + upgradeId + '}';
    }
}
