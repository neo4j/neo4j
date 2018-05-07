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

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;

public class EventHandlersTest
{
    @Test
    public void shouldCallAllEventHandlers()
    {
        // given
        StoringEventHandler handler1 = new StoringEventHandler();
        StoringEventHandler handler2 = new StoringEventHandler();

        EventHandlers eventHandlers = new EventHandlers();
        eventHandlers.add( handler1 );
        eventHandlers.add( handler2 );

        // when
        eventHandlers.on( EventHandler.EventState.Begin, "1" );
        eventHandlers.on( EventHandler.EventState.Begin, "2" );

        // then
        assertThat( handler1.messages, Matchers.contains( "1", "2" ) );
        assertThat( handler2.messages, Matchers.contains( "1", "2" ) );
    }

    private class StoringEventHandler implements EventHandler
    {
        List<String> messages = new ArrayList<>();

        @Override
        public void on( EventState eventState, String message, Throwable throwable, Param... params )
        {
            messages.add( message );
        }
    }
}
