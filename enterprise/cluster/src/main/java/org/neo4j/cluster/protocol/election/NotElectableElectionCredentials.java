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
package org.neo4j.cluster.protocol.election;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Election credentials stating that this instance cannot be elected. The vote still counts towards the total though.
 */
public final class NotElectableElectionCredentials implements ElectionCredentials, Externalizable
{
    // For Externalizable
    public NotElectableElectionCredentials()
    {}

    @Override
    public int compareTo( Object o )
    {
        return -1;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == null )
        {
            return false;
        }
        if ( !(obj instanceof NotElectableElectionCredentials) )
        {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
    }
}
