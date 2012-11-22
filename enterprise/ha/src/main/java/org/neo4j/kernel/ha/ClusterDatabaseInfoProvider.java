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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.com.ServerUtil;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.member.HighAvailabilityMembers;
import org.neo4j.kernel.impl.core.LastTxIdGetter;
import org.neo4j.management.ClusterDatabaseInfo;
import org.neo4j.management.ClusterMemberInfo;

public class ClusterDatabaseInfoProvider
{
    private URI me;
    private final HighAvailabilityMembers members;
    private final LastTxIdGetter txIdGetter;
    private final LastUpdateTime lastUpdateTime;

    public ClusterDatabaseInfoProvider( ClusterClient clusterClient, HighAvailabilityMembers members,
                                        LastTxIdGetter txIdGetter, LastUpdateTime lastUpdateTime )
    {
        this.members = members;
        this.txIdGetter = txIdGetter;
        this.lastUpdateTime = lastUpdateTime;
        clusterClient.addBindingListener( new BindingListener()
        {
            @Override
            public void listeningAt( URI uri )
            {
                me = uri;
            }
        } );
    }

    public ClusterDatabaseInfo getInfo()
    {
        for ( ClusterMemberInfo info : members.getMembers() )
        {
            if ( info.getClusterId().equals( me.toString() ) )
            {
                List<URI> uris = new ArrayList<URI>( info.getUris().length );
                for (String uri : info.getUris())
                {
                    uris.add( URI.create( uri ) );
                }
                URI haURI = ServerUtil.getUriForScheme( "ha", uris );
                int serverId = -1;
                if ( haURI != null )
                {
                    serverId = HighAvailabilityModeSwitcher.getServerId( haURI);
                }
                return new ClusterDatabaseInfo ( new ClusterMemberInfo( info.getClusterId(), info.isAvailable(),
                        info.isAlive(), info.getHaRole(), info.getClusterRoles(), info.getUris() ), serverId,
                        txIdGetter.getLastTxId(), lastUpdateTime.getLastUpdateTime() );
            }
        }

        return new ClusterDatabaseInfo( new ClusterMemberInfo("-1", false, false, "UNKNOWN", new String[0],
                new String[0] ), -1, 0, 0 );
    }
}
