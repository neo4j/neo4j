/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.storageengine.api.lock.ResourceType;

public interface ActiveLock
{
    String SHARED_MODE = "SHARED";
    String EXCLUSIVE_MODE = "EXCLUSIVE";

    String mode();

    ResourceType resourceType();

    long resourceId();

    static ActiveLock exclusiveLock( ResourceType resourceType, long resourceId )
    {
        return new Implementation( resourceType, resourceId )
        {
            @Override
            public String mode()
            {
                return EXCLUSIVE_MODE;
            }
        };
    }

    static ActiveLock sharedLock( ResourceType resourceType, long resourceId )
    {
        return new Implementation( resourceType, resourceId )
        {
            @Override
            public String mode()
            {
                return SHARED_MODE;
            }
        };
    }

    interface Factory
    {
        Factory SHARED_LOCK = ActiveLock::sharedLock;
        Factory EXCLUSIVE_LOCK = ActiveLock::exclusiveLock;

        ActiveLock create( ResourceType resourceType, long resourceId );
    }

    abstract class Implementation implements ActiveLock
    {
        private final ResourceType resourceType;
        private final long resourceId;

        private Implementation( ResourceType resourceType, long resourceId )
        {
            this.resourceType = resourceType;
            this.resourceId = resourceId;
        }

        @Override
        public abstract String mode();

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
            return resourceId == that.resourceId() &&
                    Objects.equals( mode(), that.mode() ) &&
                    Objects.equals( resourceType, that.resourceType() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( resourceType, resourceId, mode() );
        }
    }
}
