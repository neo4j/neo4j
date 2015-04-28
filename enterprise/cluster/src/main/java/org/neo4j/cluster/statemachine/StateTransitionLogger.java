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

import org.neo4j.cluster.protocol.heartbeat.HeartbeatState;
import org.neo4j.helpers.Strings;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.cluster.com.message.Message.CONVERSATION_ID;
import static org.neo4j.cluster.com.message.Message.FROM;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE;

/**
 * Logs state transitions in {@link StateMachine}s. Use this for debugging mainly.
 */
public class StateTransitionLogger
        implements StateTransitionListener
{
    private final Logging logging;

    /** Throttle so don't flood occurrences of the same message over and over */
    private String lastLogMessage = "";

    public StateTransitionLogger( Logging logging )
    {
        this.logging = logging;
    }

    @Override
    public void stateTransition( StateTransition transition )
    {
        StringLogger logger = logging.getMessagesLog( transition.getOldState().getClass() );

        if ( logger.isDebugEnabled() )
        {
            if ( transition.getOldState() == HeartbeatState.heartbeat )
            {
                return;
            }

            // The bulk of the message
            String state = transition.getOldState().getClass().getSuperclass().getSimpleName();
            StringBuilder line = new StringBuilder( state ).append( ": " ).append( transition );

            // Who was this message from?
            if ( transition.getMessage().hasHeader( FROM ) )
            {
                line.append( " from:" ).append( transition.getMessage().getHeader( FROM ) );
            }

            if ( transition.getMessage().hasHeader( INSTANCE ) )
            {
                line.append( " instance:" ).append( transition.getMessage().getHeader( INSTANCE ) );
            }

            if ( transition.getMessage().hasHeader( CONVERSATION_ID ) )
            {
                line.append( " conversation-id:" ).append( transition.getMessage().getHeader( CONVERSATION_ID ) );
            }

            Object payload = transition.getMessage().getPayload();
            if ( payload != null )
            {
                line.append( " payload:" ).append( Strings.prettyPrint( payload ) );
            }

            // Throttle
            String msg = line.toString();
            if( msg.equals( lastLogMessage ) )
            {
                return;
            }

            // Log it
            logger.debug( line.toString() );
            lastLogMessage = msg;
        }
    }
}
