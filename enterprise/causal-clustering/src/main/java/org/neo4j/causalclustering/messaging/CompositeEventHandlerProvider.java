/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
