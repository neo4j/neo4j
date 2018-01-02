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

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.election.ElectionCredentials;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentials;
import org.neo4j.kernel.ha.HighAvailabilityMemberInfoProvider;
import org.neo4j.kernel.impl.core.LastTxIdGetter;

/**
 * ElectionCredentialsProvider that provides the server id, latest txId and current role status as credentials for
 * elections.
 */
public class DefaultElectionCredentialsProvider
    implements ElectionCredentialsProvider
{
    private final InstanceId serverId;
    private final LastTxIdGetter lastTxIdGetter;
    private final HighAvailabilityMemberInfoProvider masterInfo;

    public DefaultElectionCredentialsProvider( InstanceId serverId, LastTxIdGetter lastTxIdGetter,
                                               HighAvailabilityMemberInfoProvider masterInfo )
    {
        this.serverId = serverId;
        this.lastTxIdGetter = lastTxIdGetter;
        this.masterInfo = masterInfo;
    }

    @Override
    public ElectionCredentials getCredentials( String role )
    {
        if ( masterInfo.getHighAvailabilityMemberState().isEligibleForElection() )
        {
            return new DefaultElectionCredentials(
                    serverId.toIntegerIndex(),
                    lastTxIdGetter.getLastTxId(),
                    isMasterOrToMaster() );
        }
        return new NotElectableElectionCredentials();
    }

    private boolean isMasterOrToMaster()
    {
        return masterInfo.getHighAvailabilityMemberState() == HighAvailabilityMemberState.MASTER ||
                masterInfo.getHighAvailabilityMemberState() == HighAvailabilityMemberState.TO_MASTER;
    }
}
