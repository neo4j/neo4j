/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.messaging;

import org.junit.Test;

import java.util.Map;

import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.logging.Log;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings( "unchecked" )
public class MessageProcessingHandlerTest
{
    @Test
    public void shouldCallHaltOnUnexpectedFailures() throws Exception
    {
        // Given
        BoltResponseMessageHandler msgHandler = mock( BoltResponseMessageHandler.class );
        doThrow( new RuntimeException( "Something went horribly wrong" ) )
                .when( msgHandler )
                .onSuccess( any(Map.class) );

        BoltWorker worker = mock( BoltWorker.class );
        MessageProcessingHandler handler =
                new MessageProcessingHandler( msgHandler, mock( Runnable.class ),
                        worker, mock( Log.class ) );

        // When
        handler.onFinish();

        // Then
        verify( worker  ).halt();
    }
}
