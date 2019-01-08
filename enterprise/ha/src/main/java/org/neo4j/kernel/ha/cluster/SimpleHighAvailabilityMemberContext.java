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
