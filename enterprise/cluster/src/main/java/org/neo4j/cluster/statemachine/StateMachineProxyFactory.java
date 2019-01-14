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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.StateMachines;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * Used to generate dynamic proxies whose methods are backed by a {@link StateMachine}. Method
 * calls will be translated to the corresponding message, and the parameters are set as payload.
 * <p>
 * Methods in the interface to be proxied can either return void or Future<T>. If a method returns
 * a future, then the value of it will be set when a message named nameResponse or nameFailure is created,
 * where "name" corresponds to the name of the method.
 */
public class StateMachineProxyFactory
        implements MessageProcessor
{
    private final StateMachines stateMachines;
    private final StateMachineConversations conversations;
    private final Log log;
    private volatile InstanceId me;

    private final Map<String, ResponseFuture> responseFutureMap = new ConcurrentHashMap<>();

    public StateMachineProxyFactory( StateMachines stateMachines, StateMachineConversations conversations,
            InstanceId me, LogProvider logProvider )
    {
        this.stateMachines = stateMachines;
        this.conversations = conversations;
        this.me = me;
        this.log = logProvider.getLog( getClass() );
    }

    public <CLIENT> CLIENT newProxy( Class<CLIENT> proxyInterface )
            throws IllegalArgumentException
    {
        // Get the state machine whose messages correspond to the methods of the proxy interface
        StateMachine stateMachine = getStateMachine( proxyInterface );

        // Create a new dynamic proxy and handler that converts calls to state machine invocations
        return proxyInterface.cast( Proxy.newProxyInstance( proxyInterface.getClassLoader(),
                new Class<?>[]{proxyInterface}, new StateMachineProxyHandler( this, stateMachine ) ) );
    }

    Object invoke( StateMachine stateMachine, Method method, Object arg )
    {
        if ( method.getName().equals( "toString" ) )
        {
            return me.toString();
        }

        if ( method.getName().equals( "equals" ) )
        {
            return ((StateMachineProxyHandler) Proxy.getInvocationHandler( arg )).getStateMachineProxyFactory().me.equals( me );
        }

        String conversationId = conversations.getNextConversationId();

        try
        {
            MessageType typeAsEnum = (MessageType) Enum.valueOf( stateMachine.getMessageType(), method.getName() );
            Message<?> message = Message.internal( typeAsEnum, arg );
            if ( me != null )
            {
                message.
                    setHeader( Message.HEADER_CONVERSATION_ID, conversationId ).
                    setHeader( Message.HEADER_CREATED_BY,me.toString() );
            }

            if ( method.getReturnType().equals( Void.TYPE ) )
            {
                stateMachines.process( message );
                return null;
            }
            else
            {
                ResponseFuture future = new ResponseFuture( conversationId, typeAsEnum, responseFutureMap );
                responseFutureMap.put( conversationId, future );
                log.debug( "Added response future for conversation id %s", conversationId );
                stateMachines.process( message );

                return future;
            }
        }
        catch ( IllegalArgumentException e )
        {
            throw new IllegalStateException( "No state machine can handle the method " + method.getName() );
        }
    }

    @Override
    public boolean process( Message<?> message )
    {
        if ( !responseFutureMap.isEmpty() )
        {
            if ( !message.hasHeader( Message.HEADER_TO ) )
            {
                String conversationId = message.getHeader( Message.HEADER_CONVERSATION_ID );
                ResponseFuture future = responseFutureMap.get( conversationId );
                if ( future != null )
                {
                    if ( future.setPotentialResponse( message ) )
                    {
                        responseFutureMap.remove( conversationId );
                    }
                }
                else
                {
                    log.warn(  "Unable to find the client (with the conversation id %s) waiting for the response %s.",
                            conversationId, message  );
                }
            }
        }
        return true;
    }

    private StateMachine getStateMachine( Class<?> proxyInterface )
            throws IllegalArgumentException
    {
        IllegalArgumentException exception = new IllegalArgumentException( "No state machine can handle the " +
                "interface:" + proxyInterface.getName() );

        statemachine:
        for ( StateMachine stateMachine : stateMachines.getStateMachines() )
        {
            boolean foundMatch = false;

            for ( Method method : proxyInterface.getMethods() )
            {
                if ( !(method.getReturnType().equals( Void.TYPE ) || method.getReturnType().equals( Future.class )) )
                {
                    throw new IllegalArgumentException( "Methods must return either void or Future" );
                }

                try
                {
                    Enum.valueOf( stateMachine.getMessageType(), method.getName() );

                    // Ok!
                    foundMatch = true;
                }
                catch ( Exception e )
                {
                    if ( foundMatch )
                    // State machine could only partially handle this interface
                    {
                        exception = new IllegalArgumentException(
                                "State machine for " + stateMachine.getMessageType().getName() +
                                " cannot handle method:" + method.getName() );
                    }

                    // Continue searching
                    continue statemachine;
                }
            }

            // All methods are implemented by this state machine - return it!
            return stateMachine;
        }

        // Could not find any state machine that can handle this interface
        throw exception;
    }

    private static class ResponseFuture implements Future<Object>
    {
        private final String conversationId;
        private final MessageType initiatedByMessageType;
        private final Map<String,ResponseFuture> responseFutureMap; /* temporary for debug logging */

        private Message response;

        ResponseFuture( String conversationId, MessageType initiatedByMessageType,
                Map<String,ResponseFuture> responseFutureMap )
        {
            this.conversationId = conversationId;
            this.initiatedByMessageType = initiatedByMessageType;
            this.responseFutureMap = responseFutureMap;
        }

        synchronized boolean setPotentialResponse( Message response )
        {
            if ( isResponse( response ) )
            {
                this.response = response;
                this.notifyAll();
                return true;
            }
            else
            {
                return false;
            }
        }

        private boolean isResponse( Message response )
        {
            return response.getMessageType().name().equals( initiatedByMessageType.name() + "Response" ) ||
                   response.getMessageType().name().equals( initiatedByMessageType.name() + "Failure" );
        }

        @Override
        public boolean cancel( boolean mayInterruptIfRunning )
        {
            return false;
        }

        @Override
        public boolean isCancelled()
        {
            return false;
        }

        @Override
        public boolean isDone()
        {
            return response != null;
        }

        @Override
        public synchronized Object get()
                throws InterruptedException, ExecutionException
        {
            if ( response != null )
            {
                return getResult();
            }

            while ( response == null )
            {
                this.wait( 50 );
            }

            return getResult();
        }

        private synchronized Object getResult()
                throws InterruptedException, ExecutionException
        {
            if ( response.getMessageType().name().equals( initiatedByMessageType.name() + "Failure" ) )
            {
                // Call failed
                if ( response.getPayload() != null )
                {
                    if ( response.getPayload() instanceof Throwable )
                    {
                        throw new ExecutionException( (Throwable) response.getPayload() );
                    }
                    else
                    {
                        throw new InterruptedException( response.getPayload().toString() );
                    }
                }
                else
                {
                    // No message specified
                    throw new InterruptedException();
                }
            }
            else
            {
                // Return result
                return response.getPayload();
            }
        }

        @Override
        public synchronized Object get( long timeout, TimeUnit unit )
                throws InterruptedException, ExecutionException, TimeoutException
        {
            if ( response != null )
            {
                getResult();
            }

            this.wait( unit.toMillis( timeout ) );

            if ( response == null )
            {
                throw new TimeoutException( format( "Conversation-response mapping:%n" + responseFutureMap ) );
            }
            return getResult();
        }

        @Override
        public String toString()
        {
            return "ResponseFuture{" + "conversationId='" + conversationId + '\'' + ", initiatedByMessageType=" +
                    initiatedByMessageType + ", response=" + response + '}';
        }
    }
}
