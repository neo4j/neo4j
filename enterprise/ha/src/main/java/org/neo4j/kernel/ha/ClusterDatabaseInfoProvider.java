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
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.kernel.ha.cluster.member.HighAvailabilityMembers;
import org.neo4j.management.ClusterDatabaseInfo;
import org.neo4j.management.ClusterMemberInfo;

public class ClusterDatabaseInfoProvider
{
    private URI me;
    private final HighAvailabilityMembers members;

    public ClusterDatabaseInfoProvider( ClusterClient clusterClient, HighAvailabilityMembers members )
    {
        this.members = members;
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
        for ( ClusterMemberInfo member : members.getMembers() )
        {
            if ( member.getInstanceId().equals( me.toString() ) )
            {
                return new ClusterDatabaseInfo( member, 0, 0 );
            }
        }
        
        // TODO return something instead of throwing exception, right?
        throw new IllegalStateException( "Couldn't find any information about myself, can't be right" );
    }
}
