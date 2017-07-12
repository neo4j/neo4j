/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

public final class StoreId implements Externalizable
{

    public static final StoreId DEFAULT = new StoreId( -1, -1, -1);

    private static final Random r = new SecureRandom();

    private long creationTime;
    private long randomId;
    private long storeVersion;

    private StoreId()
    {
        //For the readExternal method.
    }

    public StoreId( long storeVersion )
    {
        long currentTimeMillis = System.currentTimeMillis();
        long randomLong = r.nextLong();
        this.storeVersion = storeVersion;
        this.creationTime = currentTimeMillis;
        this.randomId = randomLong;
    }

    public StoreId( long creationTime, long randomId, long storeVersion )
    {
        this.creationTime = creationTime;
        this.randomId = randomId;
        this.storeVersion = storeVersion;
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
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        creationTime = in.readLong();
        randomId = in.readLong();
        storeVersion = in.readLong();
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
                '}';
    }

    private static boolean equal( long first, long second )
    {
        return first == second || first == -1 || second == -1;
    }
}
