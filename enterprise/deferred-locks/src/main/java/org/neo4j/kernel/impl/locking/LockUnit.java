/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.locking;

import org.neo4j.storageengine.api.lock.ResourceType;

/**
 * Description of a lock that was deferred to commit time.
 */
public class LockUnit implements Comparable<LockUnit>, ActiveLock
{
    private final ResourceType resourceType;
    private final long resourceId;
    private final boolean exclusive;

    public LockUnit( ResourceType resourceType, long resourceId, boolean exclusive )
    {
        this.resourceType = resourceType;
        this.resourceId = resourceId;
        this.exclusive = exclusive;
    }

    @Override
    public String mode()
    {
        return exclusive ? EXCLUSIVE_MODE : SHARED_MODE;
    }

    @Override
    public ResourceType resourceType()
    {
        return resourceType;
    }

    @Override
    public long resourceId()
    {
        return resourceId;
    }

    public boolean isExclusive()
    {
        return exclusive;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (exclusive ? 1231 : 1237);
        result = prime * result + (int) (resourceId ^ (resourceId >>> 32));
        result = prime * result + resourceType.hashCode();
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        LockUnit other = (LockUnit) obj;
        if ( exclusive != other.exclusive )
        {
            return false;
        }
        if ( resourceId != other.resourceId )
        {
            return false;
        }
        else if ( resourceType.typeId() != other.resourceType.typeId() )
        {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo( LockUnit o )
    {
        // Exclusive locks go first to minimize amount of potential deadlocks
        int exclusiveCompare = Boolean.compare( exclusive, o.exclusive );
        if ( exclusiveCompare != 0 )
        {
            return -exclusiveCompare;
        }

        // Then shared/exclusive locks are compared by resourceTypeId and then by resourceId
        return resourceType.typeId() == o.resourceType.typeId() ? Long.compare( resourceId, o.resourceId )
                                                                : resourceType.typeId() - o.resourceType.typeId();
    }

    @Override
    public String toString()
    {
        return "Resource [resourceType=" + resourceType + ", resourceId=" + resourceId + ", exclusive=" + exclusive
               + "]";
    }
}
