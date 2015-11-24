/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.discovery;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.cluster.InstanceId;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.configuration.Config;

public class TestOnlyDiscoveryServiceFactory implements DiscoveryServiceFactory
{
    protected final Set<CoreMember> coreMembers = new TreeSet<>( ( o1, o2 ) -> {
        if ( o1 == null && o2 == null )
        {
            return 0;
        }
        if ( o1 == null )
        {
            return -1;
        }
        if ( o2 == null )
        {
            return 1;
        }

        int port1 = o1.getCoreAddress().socketAddress().getPort();
        int port2 = o2.getCoreAddress().socketAddress().getPort();
        if ( port1 < port2 )
        {
            return -1;
        }
        else if ( port1 == port2 )
        {
            return 0;
        }
        else
        {
            return 1;
        }
    } );

    protected Set<CoreDiscoveryService.Listener> membershipListeners = new CopyOnWriteArraySet<>();
    protected CoreMember bootstrappable;

    protected final Set<InstanceId> edgeMembers = new CopyOnWriteArraySet<>();

    @Override
    public CoreDiscoveryService coreDiscoveryService( Config config )
    {
        return new TestOnlyCoreDiscoveryService( config, this );
    }

    @Override
    public EdgeDiscoveryService edgeDiscoveryService( Config config )
    {
        return new TestOnlyEdgeDiscoveryService( config, this );
    }
}
