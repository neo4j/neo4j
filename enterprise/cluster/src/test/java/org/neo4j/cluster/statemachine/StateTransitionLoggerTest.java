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
package org.neo4j.cluster.statemachine;

import org.junit.Test;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

import static org.mockito.Mockito.*;
import static org.neo4j.cluster.protocol.cluster.ClusterMessage.join;
import static org.neo4j.cluster.protocol.cluster.ClusterState.entered;
import static org.neo4j.cluster.protocol.cluster.ClusterState.joining;

public class StateTransitionLoggerTest
{
    @Test
    public void shouldThrottle() throws Exception
    {
        // Given
        Logging logging = mock( Logging.class );
        StringLogger logger = mock(StringLogger.class);
        when(logging.getMessagesLog( any(Class.class) )).thenReturn( logger );
        when(logger.isDebugEnabled()).thenReturn( true );

        StateTransitionLogger stateLogger = new StateTransitionLogger( logging );

        // When
        stateLogger.stateTransition( new StateTransition( entered, Message.internal( join), joining ) );
        stateLogger.stateTransition( new StateTransition( entered, Message.internal( join), joining ) );
        stateLogger.stateTransition( new StateTransition( joining, Message.internal( join), entered ) );
        stateLogger.stateTransition( new StateTransition( entered, Message.internal( join), joining ) );

        // Then
        verify( logger, times(4) ).isDebugEnabled();
        verify( logger, times(2) ).debug( "ClusterState: entered-[join]->joining" );
        verify( logger ).debug( "ClusterState: joining-[join]->entered" );
        verifyNoMoreInteractions( logger );
    }
}
