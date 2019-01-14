/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cluster.statemachine;

import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatState;
import org.neo4j.helpers.Strings;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.cluster.com.message.Message.HEADER_CONVERSATION_ID;
import static org.neo4j.cluster.com.message.Message.HEADER_FROM;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE;

/**
 * Logs state transitions in {@link StateMachine}s. Use this for debugging mainly.
 */
public class StateTransitionLogger
        implements StateTransitionListener
{
    private final LogProvider logProvider;
    private AtomicBroadcastSerializer atomicBroadcastSerializer;

    /** Throttle so don't flood occurrences of the same message over and over */
    private String lastLogMessage = "";

    public StateTransitionLogger( LogProvider logProvider, AtomicBroadcastSerializer atomicBroadcastSerializer )
    {
        this.logProvider = logProvider;
        this.atomicBroadcastSerializer = atomicBroadcastSerializer;
    }

    @Override
    public void stateTransition( StateTransition transition )
    {
        Log log = logProvider.getLog( transition.getOldState().getClass() );

        if ( log.isDebugEnabled() )
        {
            if ( transition.getOldState() == HeartbeatState.heartbeat )
            {
                return;
            }

            // The bulk of the message
            String state = transition.getOldState().getClass().getSuperclass().getSimpleName();
            StringBuilder line = new StringBuilder( state ).append( ": " ).append( transition );

            // Who was this message from?
            if ( transition.getMessage().hasHeader( HEADER_FROM ) )
            {
                line.append( " from:" ).append( transition.getMessage().getHeader( HEADER_FROM ) );
            }

            if ( transition.getMessage().hasHeader( INSTANCE ) )
            {
                line.append( " instance:" ).append( transition.getMessage().getHeader( INSTANCE ) );
            }

            if ( transition.getMessage().hasHeader( HEADER_CONVERSATION_ID ) )
            {
                line.append( " conversation-id:" ).append( transition.getMessage().getHeader( HEADER_CONVERSATION_ID ) );
            }

            Object payload = transition.getMessage().getPayload();
            if ( payload != null )
            {
                if ( payload instanceof Payload )
                {
                    try
                    {
                        payload = atomicBroadcastSerializer.receive( (Payload) payload );
                    }
                    catch ( Throwable e )
                    {
                        // Ignore
                    }
                }

                line.append( " payload:" ).append( Strings.prettyPrint( payload ) );
            }

            // Throttle
            String msg = line.toString();
            if ( msg.equals( lastLogMessage ) )
            {
                return;
            }

            // Log it
            log.debug( line.toString() );
            lastLogMessage = msg;
        }
    }
}
