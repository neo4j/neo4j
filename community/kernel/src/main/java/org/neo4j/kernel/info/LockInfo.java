/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.info;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class LockInfo
{
    private final String resource;
    private final ResourceType type;
    private final int readCount;
    private final int writeCount;
    private final List<WaitingThread> waitingThreads;
    private final LockingTransaction[] lockingTxs;

    @ConstructorProperties( { "resourceType", "resourceId", "readCount", "writeCount", "lockingTransactions",
            "waitingThreads" } )
    public LockInfo( ResourceType type, String resourceId, int readCount, int writeCount,
            List<LockingTransaction> locking, List<WaitingThread> waiting )
    {
        this.type = type;
        this.resource = resourceId;
        this.readCount = readCount;
        this.writeCount = writeCount;
        this.lockingTxs = locking.toArray( new LockingTransaction[locking.size()] );
        this.waitingThreads = new ArrayList<WaitingThread>( waiting );
    }

    public LockInfo( ResourceType type, String resourceId, int readCount, int writeCount,
            Collection<LockingTransaction> locking )
    {
        this.type = type;
        this.resource = resourceId;
        this.readCount = readCount;
        this.writeCount = writeCount;
        this.waitingThreads = new ArrayList<WaitingThread>();
        this.lockingTxs = locking.toArray( new LockingTransaction[locking.size()] );
        for ( LockingTransaction tx : this.lockingTxs )
        {
            if ( tx instanceof WaitingThread )
            {
                waitingThreads.add( (WaitingThread) tx );
            }
        }
    }

    @Override
    public String toString()
    {
        return new StringBuilder( type.toString( resource ) ).append( "{readCount=" ).append( readCount ).append(
                ", writeCount=" ).append( writeCount ).append( "}" ).toString();
    }

    public ResourceType getResourceType()
    {
        return type;
    }

    public String getResourceId()
    {
        return resource;
    }

    public int getWriteCount()
    {
        return writeCount;
    }

    public int getReadCount()
    {
        return readCount;
    }

    public int getWaitingThreadsCount()
    {
        return waitingThreads.size();
    }

    public List<WaitingThread> getWaitingThreads()
    {
        return Collections.unmodifiableList( waitingThreads );
    }

    public List<LockingTransaction> getLockingTransactions()
    {
        return Collections.unmodifiableList( Arrays.asList( lockingTxs ) );
    }
}
