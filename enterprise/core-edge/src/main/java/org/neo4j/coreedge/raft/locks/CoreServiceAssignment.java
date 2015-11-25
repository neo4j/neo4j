/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.locks;

import java.util.Objects;
import java.util.UUID;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.server.CoreMember;

import static java.lang.String.format;

public class CoreServiceAssignment implements ReplicatedContent
{
    private final CoreServiceRegistry.ServiceType serviceType;
    private final CoreMember provider;
    private UUID assignmentId;

    public CoreServiceAssignment( CoreServiceRegistry.ServiceType serviceType, CoreMember provider, UUID assignmentId )
    {
        this.serviceType = serviceType;
        this.provider = provider;
        this.assignmentId = assignmentId;
    }

    public CoreServiceRegistry.ServiceType serviceType()
    {
        return serviceType;
    }

    public CoreMember provider()
    {
        return provider;
    }

    public UUID assignmentId()
    {
        return assignmentId;
    }

    @Override
    public String toString()
    {
        return format( "CoreServiceAssignment{serviceType=%s, provider=%s, assignmentId=%s}",
                serviceType, provider, assignmentId );
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
        CoreServiceAssignment that = (CoreServiceAssignment) o;
        return Objects.equals( serviceType, that.serviceType ) &&
                Objects.equals( provider, that.provider ) &&
                Objects.equals( assignmentId, that.assignmentId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( serviceType, provider, assignmentId );
    }

}
