/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.helpers.AdvertisedSocketAddress;

class CatchUpChannelPool<CHANNEL extends CatchUpChannelPool.Channel>
{
    private final Map<AdvertisedSocketAddress, LinkedList<CHANNEL>> idleChannels = new HashMap<>();
    private final Set<CHANNEL> activeChannels = new HashSet<>();
    private final Function<AdvertisedSocketAddress, CHANNEL> factory;

    CatchUpChannelPool( Function<AdvertisedSocketAddress, CHANNEL> factory )
    {
        this.factory = factory;
    }

    synchronized CHANNEL acquire( AdvertisedSocketAddress catchUpAddress )
    {
        CHANNEL channel;
        LinkedList<CHANNEL> channels = idleChannels.get( catchUpAddress );
        if ( channels == null )
        {
            channel = factory.apply( catchUpAddress );
        }
        else
        {
            channel = channels.poll();
            if ( channels.isEmpty() )
            {
                idleChannels.remove( catchUpAddress );
            }
        }

        activeChannels.add( channel );
        return channel;
    }

    synchronized void dispose( CHANNEL channel )
    {
        activeChannels.remove( channel );
        channel.close();
    }

    synchronized void release( CHANNEL channel )
    {
        activeChannels.remove( channel );
        idleChannels.computeIfAbsent( channel.destination(), (address) -> new LinkedList<>() ).add( channel );
    }

    synchronized void close()
    {
        idleChannels.values().stream().flatMap( Collection::stream )
                .forEach( Channel::close );
        activeChannels.forEach( Channel::close );
    }

    interface Channel
    {
        AdvertisedSocketAddress destination();

        void close();
    }
}
