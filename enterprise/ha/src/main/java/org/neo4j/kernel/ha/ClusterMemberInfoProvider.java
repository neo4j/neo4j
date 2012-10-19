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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.com.ServerUtil;
import org.neo4j.kernel.ha.cluster.ClusterEventListener;
import org.neo4j.kernel.ha.cluster.ClusterEvents;
import org.neo4j.kernel.ha.cluster.ClusterMemberModeSwitcher;
import org.neo4j.management.ClusterMemberInfo;

public class ClusterMemberInfoProvider
{
    private final Map<URI, ClusterMemberInfo> memberInfo = new HashMap<URI, ClusterMemberInfo>();

    public ClusterMemberInfoProvider(ClusterEvents events)
    {
        events.addClusterEventListener( new ClusterMemberInfoLister() );
    }
    public ClusterMemberInfo[] getClusterInfo()
    {
        return memberInfo.values().toArray( new ClusterMemberInfo[]{} );
    }

    private class ClusterMemberInfoLister implements ClusterEventListener
    {
        @Override
        public void masterIsElected( URI masterUri )
        {
            // no op, only care about members
        }

        @Override
        public void memberIsAvailable( String role, URI instanceClusterUri, Iterable<URI> instanceUris )
        {
            int instanceId =
                    ClusterMemberModeSwitcher.getServerId( ServerUtil.getUriForScheme( "ha", instanceUris ));
            List<String> uris = new ArrayList<String>( 2 );
            for ( URI instanceUri : instanceUris )
            {
                uris.add( instanceUri.toASCIIString() );
            }
            memberInfo.put( instanceClusterUri,
                    new ClusterMemberInfo( Integer.toString( instanceId ), role, new String[] {role},
                            uris.toArray( new String[uris.size()] ) ));
        }
    }
}
