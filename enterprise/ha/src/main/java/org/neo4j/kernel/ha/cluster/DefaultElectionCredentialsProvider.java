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
