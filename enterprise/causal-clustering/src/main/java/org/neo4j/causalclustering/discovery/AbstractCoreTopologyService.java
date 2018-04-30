/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.discovery;

import java.util.Objects;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public abstract class AbstractCoreTopologyService extends SafeLifecycle implements CoreTopologyService
{
    protected final CoreTopologyListenerService listenerService = new CoreTopologyListenerService();
    protected final Config config;
    protected final MemberId myself;
    protected final Log log;
    protected final Log userLog;

    protected volatile LeaderInfo leaderInfo = LeaderInfo.INITIAL;

    protected AbstractCoreTopologyService( Config config, MemberId myself, LogProvider logProvider, LogProvider userLogProvider )
    {
        this.config = config;
        this.myself = myself;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
    }

    @Override
    public final synchronized void addLocalCoreTopologyListener( Listener listener )
    {
        this.listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange( localCoreServers() );
    }

    @Override
    public final void removeLocalCoreTopologyListener( Listener listener )
    {
        listenerService.removeCoreTopologyListener( listener );
    }

    @Override
    public final void setLeader( LeaderInfo newLeader, String dbName )
    {
        if ( this.leaderInfo.term() < newLeader.term() && newLeader.memberId() != null )
        {
            this.leaderInfo = newLeader;
            setLeader0( newLeader, dbName );
        }
    }

    protected abstract void setLeader0( LeaderInfo newLeader, String dbName );

    @Override
    public final void handleStepDown( long term, String dbName )
    {
        boolean wasLeaderForTerm = Objects.equals( myself, leaderInfo.memberId() ) && term == leaderInfo.term();
        if ( wasLeaderForTerm )
        {
            log.info( "Step down event detected. This topology member, with MemberId %s, was leader in term %s, now moving " +
                    "to follower.", myself, leaderInfo.term() );
            handleStepDown0( term, dbName );
        }
    }

    protected abstract void handleStepDown0( long term, String dbName );
}
