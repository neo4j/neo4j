/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
    public void shouldThrottle()
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
