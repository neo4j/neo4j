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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Trigger messages when given state transitions occur
 */
public class StateMachineRules
    implements StateTransitionListener
{
    private final MessageHolder outgoing;

    private Map<State<?,?>,List<StateMachineRule>> rules = new HashMap<>();

    public StateMachineRules( MessageHolder outgoing )
    {
        this.outgoing = outgoing;
    }

    public StateMachineRules rule( State<?, ?> oldState,
                                   MessageType messageType,
                                   State<?, ?> newState,
                                   Message<?>... messages
    )
    {
        List<StateMachineRule> fromRules = rules.computeIfAbsent( oldState, k -> new ArrayList<>() );
        fromRules.add( new StateMachineRule( oldState,  messageType, newState, messages ) );
        return this;
    }

    @Override
    public void stateTransition( StateTransition transition )
    {
        List<StateMachineRule> oldStateRules = rules.get( transition.getOldState() );
        if ( oldStateRules != null )
        {
            for ( StateMachineRule oldStateRule : oldStateRules )
            {
                oldStateRule.stateTransition( transition );
            }
        }
    }

    private class StateMachineRule
        implements StateTransitionListener
    {
        State<?,?> oldState;
        MessageType messageType;
        State<?,?> newState;

        Message<?>[] messages;

        private StateMachineRule( State<?, ?> oldState, MessageType messageType, State<?, ?> newState, Message<?>[] messages )
        {
            this.oldState = oldState;
            this.messageType = messageType;
            this.newState = newState;
            this.messages = messages;
        }

        @Override
        public void stateTransition( StateTransition transition )
        {
            if ( oldState.equals( transition.getOldState() ) &&
                    transition.getMessage().getMessageType().equals( messageType ) &&
                    newState.equals( transition.getNewState() ) )
            {
                for ( Message<?> message : messages )
                {
                    outgoing.offer( message );
                }
            }
        }
    }
}
