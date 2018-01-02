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

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * A single state transition that occurred in
 * a state machine as a consequence of handling a message.
 */
public class StateTransition
{
    private State<?,?> oldState;
    private Message<? extends MessageType> message;
    private State<?,?> newState;

    public StateTransition( State<?,?> oldState, Message<? extends MessageType> message, State<?,?> newState )
    {
        this.oldState = oldState;
        this.message = message;
        this.newState = newState;
    }

    public State<?,?> getOldState()
    {
        return oldState;
    }

    public Message<? extends MessageType> getMessage()
    {
        return message;
    }

    public State<?,?> getNewState()
    {
        return newState;
    }

    @Override
    public boolean equals( Object o )
    {
        if( this == o )
        {
            return true;
        }
        if( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        StateTransition that = (StateTransition) o;

        if( !message.equals( that.message ) )
        {
            return false;
        }
        if( !newState.equals( that.newState ) )
        {
            return false;
        }
        if( !oldState.equals( that.oldState ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = oldState.hashCode();
        result = 31 * result + message.hashCode();
        result = 31 * result + newState.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        if (message.getPayload() instanceof String)
            return getOldState().toString()+
                   "-["+getMessage().getMessageType()+":"+getMessage().getPayload()+"]->"+
                   getNewState().toString();
        else
            return getOldState().toString()+
                   "-["+getMessage().getMessageType()+"]->"+
                   getNewState().toString();
    }
}
