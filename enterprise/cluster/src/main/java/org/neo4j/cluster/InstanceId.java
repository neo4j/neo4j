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
package org.neo4j.cluster;

import static org.neo4j.helpers.Uris.parameter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;

/**
 * Represents the concept of the cluster wide unique id of an instance. The
 * main requirement is total order over the instances of the class, currently
 * implemented by an encapsulated integer.
 * It is also expected to be serializable, as it's transmitted over the wire
 * as part of messages between instances.
 */
public class InstanceId implements Externalizable, Comparable<InstanceId>
{
    public static final InstanceId NONE = new InstanceId( Integer.MIN_VALUE );

    private int serverId;

    public InstanceId()
    {}

    public InstanceId( int serverId )
    {
        this.serverId = serverId;
    }

    @Override
    public int compareTo( InstanceId o )
    {
        return serverId - o.serverId;
    }

    @Override
    public int hashCode()
    {
        return serverId;
    }

    @Override
    public String toString()
    {
        return Integer.toString( serverId );
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

        InstanceId instanceId1 = (InstanceId) o;

        if ( serverId != instanceId1.serverId )
        {
            return false;
        }

        return true;
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeInt( serverId );
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        serverId = in.readInt();
    }

    public int toIntegerIndex()
    {
        return serverId;
    }

    public String instanceNameFromURI( URI member )
    {
        String name = member == null ? null : parameter( "memberName" ).apply( member );
        return name == null? toString() : name;
    }
}
