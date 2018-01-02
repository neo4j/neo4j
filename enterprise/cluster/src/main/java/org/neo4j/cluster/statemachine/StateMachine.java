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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.logging.LogProvider;

/**
 * State machine that wraps a context and a state, which can change when a Message comes in.
 * Incoming messages must be of the particular type that the state understands.
 * A state machine can only handle one message at a time, so the handle message is synchronized.
 */
public class StateMachine
{
    private Object context;
    private Class<? extends MessageType> messageEnumType;
    private State<?, ?> state;
    private LogProvider logProvider;

    private List<StateTransitionListener> listeners = new ArrayList<StateTransitionListener>();

    public StateMachine( Object context, Class<? extends MessageType> messageEnumType, State<?, ?> state,
                         LogProvider logProvider )
    {
        this.context = context;
        this.messageEnumType = messageEnumType;
        this.state = state;
        this.logProvider = logProvider;
    }

    public Class<? extends MessageType> getMessageType()
    {
        return messageEnumType;
    }

    public State<?, ?> getState()
    {
        return state;
    }

    public Object getContext()
    {
        return context;
    }

    public void addStateTransitionListener( StateTransitionListener listener )
    {
        List<StateTransitionListener> newlisteners = new ArrayList<StateTransitionListener>( listeners );
        newlisteners.add( listener );
        listeners = newlisteners;
    }

    public void removeStateTransitionListener( StateTransitionListener listener )
    {
        List<StateTransitionListener> newlisteners = new ArrayList<StateTransitionListener>( listeners );
        newlisteners.remove( listener );
        listeners = newlisteners;
    }

    public synchronized void handle( Message<? extends MessageType> message, MessageHolder outgoing )
    {
        try
        {
            // Let the old state handle the incoming message and tell us what the new state should be
            State<Object, MessageType> oldState = (State<Object, MessageType>) state;
            State<?, ?> newState = oldState.handle( (Object) context, (Message<MessageType>) message, outgoing );
            state = newState;

            // Notify any listeners of the new state
            StateTransition transition = new StateTransition( oldState, message, newState );
            for ( StateTransitionListener listener : listeners )
            {
                try
                {
                    listener.stateTransition( transition );
                }
                catch ( Throwable e )
                {
                    // Ignore
                    logProvider.getLog( listener.getClass() ).warn( "Listener threw exception", e );
                }
            }
        }
        catch ( Throwable throwable )
        {
            logProvider.getLog( getClass() ).warn( "Exception in message handling", throwable );
        }
    }

    @Override
    public String toString()
    {
        return state.toString() + ": " + context.toString();
    }
}
