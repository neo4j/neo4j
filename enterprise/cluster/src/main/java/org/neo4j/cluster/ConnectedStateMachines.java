/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cluster;

import static org.neo4j.cluster.com.message.Message.CONVERSATION_ID;
import static org.neo4j.cluster.com.message.Message.CREATED_BY;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.statemachine.StateTransitionListener;
import org.neo4j.cluster.timeout.TimeoutStrategy;
import org.neo4j.cluster.timeout.Timeouts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combines a set of state machines into one. This will
 * typically receive messages from the network and then delegate
 * to the correct state machine based on what type of message comes in.
 * Only one message at a time can be processed.
 */
public class ConnectedStateMachines
        implements MessageProcessor, MessageSource
{
    private final Logger logger = LoggerFactory.getLogger( ConnectedStateMachines.class );

    private final MessageSender sender;
    private DelayedDirectExecutor executor;
    private Timeouts timeouts;
    private final Map<Class<? extends MessageType>, StateMachine> stateMachines = new LinkedHashMap<Class<? extends
            MessageType>, StateMachine>();

    private final List<MessageProcessor> outgoingProcessors = new ArrayList<MessageProcessor>();
    private final OutgoingMessageHolder outgoing;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock( true );

    public ConnectedStateMachines( MessageSource source,
                                   final MessageSender sender,
                                   TimeoutStrategy timeoutStrategy,
                                   DelayedDirectExecutor executor )
    {
        this.sender = sender;
        this.executor = executor;
        this.timeouts = new Timeouts( this, timeoutStrategy );

        outgoing = new OutgoingMessageHolder();
        source.addMessageProcessor( this );
    }

    public Timeouts getTimeouts()
    {
        return timeouts;
    }

    public synchronized void addStateMachine( StateMachine stateMachine )
    {
        stateMachines.put( stateMachine.getMessageType(), stateMachine );
    }

    public synchronized void removeStateMachine( StateMachine stateMachine )
    {
        stateMachines.remove( stateMachine.getMessageType() );
    }

    public Iterable<StateMachine> getStateMachines()
    {
        return stateMachines.values();
    }

    @Override
    public void addMessageProcessor( MessageProcessor messageProcessor )
    {
        outgoingProcessors.add( messageProcessor );
    }

    public OutgoingMessageHolder getOutgoing()
    {
        return outgoing;
    }

    @Override
    public synchronized void process( Message<? extends MessageType> message )
    {
        lock.writeLock().lock();

        try
        {
            // Lock timeouts while we are processing the message
            synchronized ( timeouts )
            {
                StateMachine stateMachine = stateMachines.get( message.getMessageType().getClass() );
                if ( stateMachine == null )
                {
                    return; // No StateMachine registered for this MessageType type - Ignore this
                }

                stateMachine.handle( message, outgoing );

                // Process and send messages
                // Allow state machines to send messages to each other as well in this loop
                Message<? extends MessageType> outgoingMessage;
                List<Message<? extends MessageType>> toSend = new LinkedList<Message<? extends MessageType>>();
                try
                {
                    while ( ( outgoingMessage = outgoing.nextOutgoingMessage() ) != null )
                    {
                        message.copyHeadersTo( outgoingMessage, CONVERSATION_ID, CREATED_BY );

                        for ( MessageProcessor outgoingProcessor : outgoingProcessors )
                        {
                            try
                            {
                                outgoingProcessor.process( outgoingMessage );
                            }
                            catch ( Throwable e )
                            {
                                logger.warn( "Outgoing message processor threw exception", e );
                            }
                        }

                        if ( outgoingMessage.hasHeader( Message.TO ) )
                        {
//                            try
//                            {
//                                sender.process( outgoingMessage );
//                            }
//                            catch ( Throwable e )
//                            {
//                                logger.warn( "Message sending threw exception", e );
//                            }
                            toSend.add( outgoingMessage );
                        }
                        else
                        {
                            // Deliver internally if possible
                            StateMachine internalStatemachine = stateMachines.get( outgoingMessage.getMessageType()
                                    .getClass() );
                            //                if (internalStatemachine != null && stateMachine != internalStatemachine )
                            if ( internalStatemachine != null )
                            {
                                internalStatemachine.handle( (Message) outgoingMessage, outgoing );
                            }
                        }
                    }
                    if ( !toSend.isEmpty() ) // the check is necessary, sender may not have started yet
                    {
                        sender.process( toSend );
                    }
                }
                catch ( Exception e )
                {
                    logger.warn( "Error processing message " + message, e );
                }
            }

            // Before returning, process delayed executions so that they are done before returning
            // This will effectively trigger all notifications created by contexts
            executor.drain();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void addStateTransitionListener( StateTransitionListener stateTransitionListener )
    {
        for ( StateMachine stateMachine : stateMachines.values() )
        {
            stateMachine.addStateTransitionListener( stateTransitionListener );
        }
    }

    public void removeStateTransitionListener( StateTransitionListener stateTransitionListener )
    {
        for ( StateMachine stateMachine : stateMachines.values() )
        {
            stateMachine.removeStateTransitionListener( stateTransitionListener );
        }
    }

    @Override
    public String toString()
    {
        List<String> states = new ArrayList<String>();
        for ( StateMachine stateMachine : stateMachines.values() )
        {
            states.add( stateMachine.getState().getClass().getSuperclass().getSimpleName() + ":" + stateMachine
                    .getState().toString() );
        }
        return states.toString();
    }

    public StateMachine getStateMachine( Class<? extends MessageType> messageType )
    {
        return stateMachines.get( messageType );
    }

    private class OutgoingMessageHolder implements MessageHolder
    {
        private Queue<Message<? extends MessageType>> outgoingMessages = new LinkedList<Message<? extends
                MessageType>>();

        @Override
        public synchronized void offer( Message<? extends MessageType> message )
        {
            outgoingMessages.offer( message );
        }

        public synchronized Message<? extends MessageType> nextOutgoingMessage()
        {
            return outgoingMessages.poll();
        }
    }
}
