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
package org.neo4j.cluster.protocol.omega.payload;

import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cluster.protocol.omega.state.State;

public final class CollectResponsePayload implements Serializable
{
    private final URI[] servers;
    private final RefreshPayload[] registry;
    private final int readNum;

    public CollectResponsePayload( URI[] servers, RefreshPayload[] registry, int readNum )
    {
        this.servers = servers;
        this.registry = registry;
        this.readNum = readNum;
    }

    public int getReadNum()
    {
        return readNum;
    }

    public static CollectResponsePayload fromRegistry( Map<URI, State> registry, int readNum )
    {
        URI[] servers = new URI[registry.size()];
        RefreshPayload[] refreshPayloads = new RefreshPayload[registry.size()];
        int currentIndex = 0;
        for (Map.Entry<URI, State> entry : registry.entrySet())
        {
            servers[currentIndex] = entry.getKey();
            refreshPayloads[currentIndex] = RefreshPayload.fromState( entry.getValue(), -1 );
            currentIndex++;
        }
        return new CollectResponsePayload( servers, refreshPayloads, readNum );
    }

    public static Map<URI, State> fromPayload( CollectResponsePayload payload )
    {
        Map<URI, State> result = new HashMap<URI, State>();
        for ( int i = 0; i < payload.servers.length; i++ )
        {
            URI server = payload.servers[i];
            State state = RefreshPayload.toState( payload.registry[i] ).other();
            result.put( server, state );
        }
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof CollectResponsePayload))
        {
            return false;
        }
        CollectResponsePayload other = (CollectResponsePayload) obj;
        return Arrays.deepEquals(servers, other.servers) && Arrays.deepEquals( registry, other.registry ) && readNum == other.readNum;
    }

    @Override
    public String toString()
    {
        StringBuffer buffer = new StringBuffer( "CollectResponsePayload[{" );
        for ( int i = 0; i < servers.length; i++ )
        {
            URI server = servers[i];
            RefreshPayload payload = registry[i];
            buffer.append( server ).append( ":" ).append( payload );
            if (i < servers.length - 1)
            {
                buffer.append( "," );
            }
        }
        buffer.append( "}, readNum=" ).append( readNum );
        return buffer.toString();
    }
}