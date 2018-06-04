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
