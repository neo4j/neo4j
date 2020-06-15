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
package org.neo4j.kernel.impl.locking;

import java.util.Objects;

import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;

public class ActiveLock
{
    private final ResourceType resourceType;
    private final LockType lockType;
    private final long resourceId;
    private final long transactionId;

    public ActiveLock( ResourceType resourceType, LockType lockType, long transactionId, long resourceId )
    {
        this.resourceType = resourceType;
        this.lockType = lockType;
        this.resourceId = resourceId;
        this.transactionId = transactionId;
    }

    public ResourceType resourceType()
    {
        return resourceType;
    }

    public long resourceId()
    {
        return resourceId;
    }

    public LockType lockType()
    {
        return lockType;
    }

    public long transactionId()
    {
        return transactionId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof ActiveLock) )
        {
            return false;
        }
        ActiveLock that = (ActiveLock) o;
        return resourceId == that.resourceId() && Objects.equals( lockType(), that.lockType() ) && Objects.equals( resourceType, that.resourceType() ) &&
                Objects.equals( transactionId, that.transactionId() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( resourceType, resourceId, lockType, transactionId );
    }
}
