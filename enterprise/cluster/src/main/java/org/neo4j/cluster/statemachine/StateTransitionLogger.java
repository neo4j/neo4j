/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.cluster.com.message.Message.FROM;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

/**
 * Logs state transitions in {@link StateMachine}s. Use this for debugging mainly.
 */
public class StateTransitionLogger
        implements StateTransitionListener
{
    private Logging logging;

    public StateTransitionLogger( Logging logging )
    {
        this.logging = logging;
    }

    public void stateTransition( StateTransition transition )
    {
        StringLogger logger = logging.getLogger( transition.getOldState().getClass().getName() );

        if ( logger.isDebugEnabled() )
        {
            // The bulk of the message
            StringBuilder line = new StringBuilder( transition.getOldState().getClass().getSuperclass().getSimpleName() +
                    ": " + transition );
            
            // Who was this message from?
            if ( transition.getMessage().hasHeader( FROM ) )
                line.append( " from:" + transition.getMessage().getHeader( FROM ) );
            
            // Log it
            logger.debug( line.toString() );
        }
    }
}