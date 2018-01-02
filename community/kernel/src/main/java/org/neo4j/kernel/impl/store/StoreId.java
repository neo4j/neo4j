/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.SecureRandom;
import java.util.Random;

import static org.neo4j.kernel.impl.store.MetaDataStore.versionStringToLong;

public final class StoreId implements Externalizable
{
    public static final long CURRENT_STORE_VERSION = versionStringToLong( MetaDataStore.ALL_STORES_VERSION );

    public static final StoreId DEFAULT = new StoreId( -1, -1, -1, -1 );

    private static final Random r = new SecureRandom();

    private long creationTime;
    private long randomId;
    private long storeVersion;
    private long upgradeTime;
    private long upgradeId;

    public StoreId()
    {
        // If creationTime == upgradeTime && randomNumber == upgradeId then store has never been upgraded
        long currentTimeMillis = System.currentTimeMillis();
        long randomLong = r.nextLong();

        this.creationTime = currentTimeMillis;
        this.randomId = randomLong;
        this.storeVersion = CURRENT_STORE_VERSION;
        this.upgradeTime = currentTimeMillis;
        this.upgradeId = randomLong;
    }

    public StoreId( long creationTime, long randomId, long upgradeTime, long upgradeId )
    {
        this( creationTime, randomId, CURRENT_STORE_VERSION, upgradeTime, upgradeId );
    }

    public StoreId( long creationTime, long randomId, long storeVersion, long upgradeTime, long upgradeId )
    {
        this.creationTime = creationTime;
        this.randomId = randomId;
        this.storeVersion = storeVersion;
        this.upgradeTime = upgradeTime;
        this.upgradeId = upgradeId;
    }

    public static StoreId from( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        StoreId storeId = new StoreId();
        storeId.readExternal( in );
        return storeId;
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

    public long getStoreVersion()
    {
        return storeVersion;
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeLong( creationTime );
        out.writeLong( randomId );
        out.writeLong( storeVersion );
        out.writeLong( upgradeTime );
        out.writeLong( upgradeId );
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        creationTime = in.readLong();
        randomId = in.readLong();
        storeVersion = in.readLong();
        upgradeTime = in.readLong();
        upgradeId = in.readLong();
    }

    public boolean equalsByUpgradeId( StoreId other )
    {
        return equal( upgradeTime, other.upgradeTime ) && equal( upgradeId, other.upgradeId );
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
        StoreId other = (StoreId) o;
        return equal( creationTime, other.creationTime ) && equal( randomId, other.randomId );
    }

    @Override
    public int hashCode()
    {
        return 31 * (int) (creationTime ^ (creationTime >>> 32)) + (int) (randomId ^ (randomId >>> 32));
    }

    @Override
    public String toString()
    {
        return "StoreId{" +
               "creationTime=" + creationTime +
               ", randomId=" + randomId +
               ", storeVersion=" + storeVersion +
               ", upgradeTime=" + upgradeTime +
               ", upgradeId=" + upgradeId +
               '}';
    }

    private static boolean equal( long first, long second )
    {
        return first == second || first == -1 || second == -1;
    }
}
