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
package org.neo4j.cluster.statemachine;

import org.junit.Test;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.logging.AssertableLogProvider;

import static org.neo4j.cluster.protocol.cluster.ClusterMessage.join;
import static org.neo4j.cluster.protocol.cluster.ClusterState.entered;
import static org.neo4j.cluster.protocol.cluster.ClusterState.joining;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class StateTransitionLoggerTest
{
    @Test
    public void shouldThrottle() throws Exception
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider( true );

        StateTransitionLogger stateLogger = new StateTransitionLogger( logProvider,
                new AtomicBroadcastSerializer( new ObjectStreamFactory(), new ObjectStreamFactory() ) );

        // When
        stateLogger.stateTransition( new StateTransition( entered, Message.internal( join), joining ) );
        stateLogger.stateTransition( new StateTransition( entered, Message.internal( join), joining ) );
        stateLogger.stateTransition( new StateTransition( joining, Message.internal( join), entered ) );
        stateLogger.stateTransition( new StateTransition( entered, Message.internal( join), joining ) );

        // Then
        logProvider.assertExactly(
                inLog( entered.getClass() ).debug( "ClusterState: entered-[join]->joining" ),
                inLog( joining.getClass() ).debug( "ClusterState: joining-[join]->entered" ),
                inLog( entered.getClass() ).debug( "ClusterState: entered-[join]->joining" )
        );
    }
}
