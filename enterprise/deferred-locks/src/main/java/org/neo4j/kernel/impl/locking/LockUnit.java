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
package org.neo4j.kernel.impl.locking;

/**
 * Description of a lock that was deferred to commit time.
 */
public class LockUnit implements Comparable<LockUnit>
{
    private final Locks.ResourceType resourceType;
    private final long resourceId;
    private final boolean exclusive;

    public LockUnit( Locks.ResourceType resourceType, long resourceId, boolean exclusive )
    {
        this.resourceType = resourceType;
        this.resourceId = resourceId;
        this.exclusive = exclusive;
    }

    public Locks.ResourceType resourceType()
    {
        return resourceType;
    }

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
