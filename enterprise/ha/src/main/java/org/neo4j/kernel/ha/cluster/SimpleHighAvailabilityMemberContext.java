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
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.InstanceId;

/**
 * Context used by the {@link HighAvailabilityMemberStateMachine}. Keeps track of what elections and previously
 * available master this cluster member has seen.
 */
public class SimpleHighAvailabilityMemberContext implements HighAvailabilityMemberContext
{
    private InstanceId electedMasterId;
    private URI availableHaMasterId;
    private final InstanceId myId;
    private boolean slaveOnly;

    public SimpleHighAvailabilityMemberContext( InstanceId myId, boolean slaveOnly )
    {
        this.myId = myId;
        this.slaveOnly = slaveOnly;
    }

    @Override
    public InstanceId getMyId()
    {
        return myId;
    }

    @Override
    public InstanceId getElectedMasterId()
    {
        return electedMasterId;
    }

    @Override
    public void setElectedMasterId( InstanceId electedMasterId )
    {
        this.electedMasterId = electedMasterId;
    }

    @Override
    public URI getAvailableHaMaster()
    {
        return availableHaMasterId;
    }

    @Override
    public void setAvailableHaMasterId( URI availableHaMasterId )
    {
        this.availableHaMasterId = availableHaMasterId;
    }

    @Override
    public boolean isSlaveOnly()
    {
        return slaveOnly;
    }

    @Override
    public String toString()
    {
        return "SimpleHighAvailabilityMemberContext{" +
                "electedMasterId=" + electedMasterId +
                ", availableHaMasterId=" + availableHaMasterId +
                ", myId=" + myId +
                ", slaveOnly=" + slaveOnly +
                '}';
    }
}
