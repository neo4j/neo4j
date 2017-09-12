/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.stream.Collectors;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.stream.Stream.concat;

class CatchUpChannelPool<CHANNEL extends CatchUpChannelPool.Channel>
{
    private final Map<AdvertisedSocketAddress,LinkedList<CHANNEL>> idleChannels = new HashMap<>();
    private final Set<CHANNEL> activeChannels = new HashSet<>();
    private final Function<AdvertisedSocketAddress,CHANNEL> factory;

    CatchUpChannelPool( Function<AdvertisedSocketAddress,CHANNEL> factory )
    {
        this.factory = factory;
    }

    CHANNEL acquire( AdvertisedSocketAddress catchUpAddress )
    {
        CHANNEL channel = getIdleChannel( catchUpAddress );

        if ( channel == null )
        {
            channel = factory.apply( catchUpAddress );
        }

        addActiveChannel( channel );

        return channel;
    }

    private synchronized CHANNEL getIdleChannel( AdvertisedSocketAddress catchUpAddress )
    {
        CHANNEL channel = null;
        LinkedList<CHANNEL> channels = idleChannels.get( catchUpAddress );
        if ( channels != null )
        {
            channel = channels.poll();
            if ( channels.isEmpty() )
            {
                idleChannels.remove( catchUpAddress );
            }
        }
        return channel;
    }

    private synchronized void addActiveChannel( CHANNEL channel )
    {
        activeChannels.add( channel );
    }

    private synchronized void removeActiveChannel( CHANNEL channel )
    {
        activeChannels.remove( channel );
    }

    void dispose( CHANNEL channel )
    {
        removeActiveChannel( channel );
        channel.close();
    }

    synchronized void release( CHANNEL channel )
    {
        removeActiveChannel( channel );
        idleChannels.computeIfAbsent( channel.destination(), address -> new LinkedList<>() ).add( channel );
    }

    void close()
    {
        collectDisposed().forEach( Channel::close );
    }

    private synchronized Set<CHANNEL> collectDisposed()
    {
        Set<CHANNEL> disposed;
        disposed = concat(
                idleChannels.values().stream().flatMap( Collection::stream ),
                activeChannels.stream() )
                .collect( Collectors.toSet() );

        idleChannels.clear();
        activeChannels.clear();
        return disposed;
    }

    interface Channel
    {
        AdvertisedSocketAddress destination();

        void close();
    }
}
