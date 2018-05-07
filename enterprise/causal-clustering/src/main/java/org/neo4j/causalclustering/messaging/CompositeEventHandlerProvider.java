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
package org.neo4j.causalclustering.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompositeEventHandlerProvider implements EventHandlerProvider
{
    private final List<EventHandlerProvider> eventHandlerProviders = new ArrayList<>();

    public static CompositeEventHandlerProvider merge( EventHandlerProvider... eventHandlerProviders )
    {
        CompositeEventHandlerProvider compositeEventHandlerProvider = new CompositeEventHandlerProvider();
        Arrays.stream( eventHandlerProviders ).forEach( compositeEventHandlerProvider::add );
        return compositeEventHandlerProvider;
    }

    @Override
    public EventHandler eventHandler( EventId id )
    {
        EventHandlers eventHandlers = new EventHandlers();
        eventHandlerProviders.forEach( eventHandlerProvider -> eventHandlers.add( eventHandlerProvider.eventHandler( id ) ) );
        return eventHandlers;
    }

    public void add( EventHandlerProvider eventHandlerProvider )
    {
        this.eventHandlerProviders.add( eventHandlerProvider );
    }
}
