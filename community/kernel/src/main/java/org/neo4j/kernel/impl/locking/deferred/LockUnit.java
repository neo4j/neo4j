package org.neo4j.kernel.impl.locking.deferred;

import org.neo4j.kernel.impl.locking.Locks;

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
        // The important thing isn't the order itself, it's the presence of an order
        // so that all lock clients gets the same order
        int exclusiveCompare = Boolean.compare( exclusive, o.exclusive );
        if ( exclusiveCompare != 0 )
        {
            return exclusiveCompare;
        }

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
