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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.InstanceId;

public class ClusterProtocolAtomicbroadcastTestUtil
{
    public static Iterable<InstanceId> ids( int size )
    {
        List<InstanceId> ids = new ArrayList<InstanceId>();
        for ( int i = 1; i <= size; i++ )
        {
            ids.add( new InstanceId( i ) );
        }
        return ids;
    }

    public static Map<InstanceId,URI> members( int size )
    {
        Map<InstanceId,URI> members = new HashMap<InstanceId,URI>();
        for ( int i = 1; i <= size; i++ )
        {
            members.put( new InstanceId( i ), URI.create( "http://localhost:" + (6000 + i) + "?serverId=" + i ) );
        }
        return members;
    }
}
