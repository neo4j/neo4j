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
 * This event represents a change in the cluster members internal state. The possible states
 * are enumerated in {@link HighAvailabilityMemberState}.
 */
public class HighAvailabilityMemberChangeEvent
{
    private final HighAvailabilityMemberState oldState;
    private final HighAvailabilityMemberState newState;
    private final InstanceId instanceId;
    private final URI serverHaUri;

    public HighAvailabilityMemberChangeEvent( HighAvailabilityMemberState oldState,
                                              HighAvailabilityMemberState newState,
                                              InstanceId instanceId, URI serverHaUri )
    {
        this.oldState = oldState;
        this.newState = newState;
        this.instanceId = instanceId;
        this.serverHaUri = serverHaUri;
    }

    public HighAvailabilityMemberState getOldState()
    {
        return oldState;
    }

    public HighAvailabilityMemberState getNewState()
    {
        return newState;
    }

    public InstanceId getInstanceId()
    {
        return instanceId;
    }

    public URI getServerHaUri()
    {
        return serverHaUri;
    }

    @Override
    public String toString()
    {
        return "HA Member State Event[ old state: " + oldState + ", new state: " + newState +
                ", server cluster URI: " + instanceId + ", server HA URI: " + serverHaUri + "]";
    }
}
