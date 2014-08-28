/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.SecureRandom;
import java.util.Random;

public final class StoreId implements Externalizable
{
    /*
     * This field represents the store version of the last Neo4j version which had the store version as part of
     * the StoreId (2.0.1). This field is now deprecated but is not removed. Fixing it to this value ensures
     * rolling upgrades can happen.
     * // TODO use NeoStore.versionStringToLong() to do the translation - currently does not work as expected
     */
    public static final long storeVersionAsLong = 13843131341501958l;

    public static final StoreId DEFAULT = new StoreId( -1, -1, -1, -1 );

    private static final Random r = new SecureRandom();

    private long creationTime;
    private long randomId;
    private long upgradeTime;
    private long upgradeId;

    public StoreId()
    {
        // If creationTime == upgradeTime && randomNumber == upgradeId then store has never been upgraded
        long currentTimeMillis = System.currentTimeMillis();
        long randomLong = r.nextLong();

        this.creationTime = currentTimeMillis;
        this.randomId = randomLong;
        this.upgradeTime = currentTimeMillis;
        this.upgradeId = randomLong;
    }

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
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeLong( creationTime );
        out.writeLong( randomId );
        out.writeLong( upgradeTime );
        out.writeLong( upgradeId );
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        creationTime = in.readLong();
        randomId = in.readLong();
        upgradeTime = in.readLong();
        upgradeId = in.readLong();
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
        return (creationTime == storeId.creationTime) && (randomId == storeId.randomId) &&
                (upgradeId == storeId.upgradeId) && (upgradeTime == storeId.upgradeTime);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (randomId ^ (randomId >>> 32));
        result = 31 * result + (int) (upgradeTime ^ (upgradeTime >>> 32));
        result = 31 * result + (int) (upgradeId ^ (upgradeId >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "StoreId{" +
                "creationTime=" + creationTime +
                ", randomId=" + randomId +
                ", upgradeTime=" + upgradeTime +
                ", upgradeId=" + upgradeId +
                '}';
    }
}
