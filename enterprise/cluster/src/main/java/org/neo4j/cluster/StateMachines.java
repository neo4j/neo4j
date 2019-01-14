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
package org.neo4j.cluster;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.statemachine.StateTransitionListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.cluster.com.message.Message.HEADER_CONVERSATION_ID;
import static org.neo4j.cluster.com.message.Message.HEADER_CREATED_BY;


/**
 * Combines a set of state machines into one. This will
 * typically receive messages from the network and then delegate
 * to the correct state machine based on what type of message comes in.
 * Only one message at a time can be processed.
 */
public class StateMachines
        implements MessageProcessor, MessageSource
{
    public interface Monitor
    {
        void beganProcessing( Message message );

        void finishedProcessing( Message message );
    }

    private final Log log;

    private final Monitor monitor;
    private final MessageSender sender;
    private DelayedDirectExecutor executor;
    private Executor stateMachineExecutor;
    private Timeouts timeouts;
    private final Map<Class<? extends MessageType>, StateMachine> stateMachines = new LinkedHashMap<>();

    private final List<MessageProcessor> outgoingProcessors = new ArrayList<>();
    private final OutgoingMessageHolder outgoing;
    // This is used to ensure fairness of message delivery
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock( true );
    private final String instanceIdHeaderValue;

    public StateMachines( LogProvider logProvider, Monitor monitor, MessageSource source,
                          final MessageSender sender,
                          Timeouts timeouts,
                          DelayedDirectExecutor executor, Executor stateMachineExecutor, InstanceId instanceId )
    {
        this.log = logProvider.getLog( getClass() );
        this.monitor = monitor;
        this.sender = sender;
        this.executor = executor;
        this.stateMachineExecutor = stateMachineExecutor;
        this.timeouts = timeouts;
        this.instanceIdHeaderValue = instanceId.toString();

        outgoing = new OutgoingMessageHolder();
        timeouts.addMessageProcessor( this );
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
    public boolean process( final Message<? extends MessageType> message )
    {
        stateMachineExecutor.execute( new Runnable()
        {
            OutgoingMessageHolder temporaryOutgoing = new OutgoingMessageHolder();

            @Override
            public void run()
            {
                monitor.beganProcessing( message );

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

                        handleMessage( stateMachine, message );

                        // Process and send messages
                        // Allow state machines to send messages to each other as well in this loop
                        Message<? extends MessageType> outgoingMessage;
                        List<Message<? extends MessageType>> toSend = new LinkedList<>();
                        try
                        {
                            while ( (outgoingMessage = outgoing.nextOutgoingMessage()) != null )
                            {
                                message.copyHeadersTo( outgoingMessage, HEADER_CONVERSATION_ID, HEADER_CREATED_BY );

                                for ( MessageProcessor outgoingProcessor : outgoingProcessors )
                                {
                                    try
                                    {
                                        if ( !outgoingProcessor.process( outgoingMessage ) )
                                        {
                                            break;
                                        }
                                    }
                                    catch ( Throwable e )
                                    {
                                        log.warn( "Outgoing message processor threw exception", e );
                                    }
                                }

                                if ( outgoingMessage.hasHeader( Message.HEADER_TO ) )
                                {
                                    outgoingMessage.setHeader( Message.HEADER_INSTANCE_ID, instanceIdHeaderValue );
                                    toSend.add( outgoingMessage );
                                }
                                else
                                {
                                    // Deliver internally if possible
                                    StateMachine internalStatemachine = stateMachines.get( outgoingMessage
                                            .getMessageType()
                                            .getClass() );
                                    if ( internalStatemachine != null )
                                    {
                                        handleMessage( internalStatemachine, outgoingMessage );
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
                            log.warn( "Error processing message " + message, e );
                        }
                    }
                }
                finally
                {
                    lock.writeLock().unlock();
                }

                // Before returning, process delayed executions so that they are done before returning
                // This will effectively trigger all notifications created by contexts
                executor.drain();
                monitor.finishedProcessing( message );
            }

            private void handleMessage( StateMachine stateMachine, Message<? extends MessageType> message )
            {
                stateMachine.handle( message, temporaryOutgoing );
                for ( Message<? extends MessageType> next; (next = temporaryOutgoing.nextOutgoingMessage()) != null; )
                {
                    outgoing.offer( next );
                }
            }
        } );
        return true;
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
        List<String> states = new ArrayList<>();
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
        private Deque<Message<? extends MessageType>> outgoingMessages = new ArrayDeque<>();

        @Override
        public synchronized void offer( Message<? extends MessageType> message )
        {
            outgoingMessages.addFirst( message );
        }

        public synchronized Message<? extends MessageType> nextOutgoingMessage()
        {
            return outgoingMessages.pollFirst();
        }
    }
}
