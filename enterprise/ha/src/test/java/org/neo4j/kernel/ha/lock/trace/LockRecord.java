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
package org.neo4j.kernel.ha.lock.trace;

import java.util.Objects;

import org.neo4j.storageengine.api.lock.ResourceType;

public class LockRecord
{
    private boolean exclusive;
    private ResourceType resourceType;
    private long resourceId;

    public static LockRecord of( boolean exclusive, ResourceType resourceType, long resourceId )
    {
        return new LockRecord( exclusive, resourceType, resourceId );
    }

    private LockRecord( boolean exclusive, ResourceType resourceType, long resourceId )
    {
        this.exclusive = exclusive;
        this.resourceType = resourceType;
        this.resourceId = resourceId;
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
        LockRecord that = (LockRecord) o;
        return exclusive == that.exclusive && resourceId == that.resourceId &&
                Objects.equals( resourceType, that.resourceType );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( exclusive, resourceType, resourceId );
    }
}
