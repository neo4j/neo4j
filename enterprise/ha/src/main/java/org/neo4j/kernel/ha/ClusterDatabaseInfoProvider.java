/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.net.URI;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.kernel.ha.cluster.ClusterEventListener;
import org.neo4j.kernel.ha.cluster.ClusterEvents;
import org.neo4j.kernel.ha.cluster.ClusterMemberState;
import org.neo4j.management.ClusterDatabaseInfo;

public class ClusterDatabaseInfoProvider
{
    private URI me;
    private String id;
    private String status;
    private String[] roles;
    private String[] uris;
    private long lastCommittedTxId;
    private long lastUpdateTime;

    public ClusterDatabaseInfoProvider( ClusterEvents events, ProtocolServer server )
    {
        events.addClusterEventListener( new ClusterDatabaseInfoProviderListener() );
        server.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                ClusterDatabaseInfoProvider.this.me = me;
            }
        } );
    }

    public ClusterDatabaseInfo getInfo()
    {
        return new ClusterDatabaseInfo( id, status, roles, uris, lastCommittedTxId, lastUpdateTime );
    }

    private class ClusterDatabaseInfoProviderListener implements ClusterEventListener
    {
        @Override
        public void masterIsElected( URI masterUri )
        {
        }

        @Override
        public void memberIsAvailable( String role, URI instanceClusterUri, Iterable<URI> instanceUris )
        {
            if ( me.equals( instanceClusterUri ) )
            {
                if ( ClusterConfiguration.COORDINATOR.equals( role ) )
                {
                    status = ClusterMemberState.MASTER.name();
                    roles = new String[] { ClusterMemberState.MASTER.name() };
                }
                if ( ClusterConfiguration.SLAVE.equals( role ) )
                {
                    status = ClusterMemberState.SLAVE.name();
                    roles = new String[] { ClusterMemberState.SLAVE.name() };
                }
            }
        }
    }
}
