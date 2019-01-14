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
    public int compareTo( ElectionCredentials o )
    {
        return -1;
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj instanceof NotElectableElectionCredentials;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public void writeExternal( ObjectOutput out )
    {
    }

    @Override
    public void readExternal( ObjectInput in )
    {
    }
}
