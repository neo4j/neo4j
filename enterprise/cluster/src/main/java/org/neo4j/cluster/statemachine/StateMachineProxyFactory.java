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
    private volatile InstanceId me;

    private final Map<String, ResponseFuture> responseFutureMap = new ConcurrentHashMap<String, ResponseFuture>();


    public StateMachineProxyFactory( StateMachines stateMachines, StateMachineConversations conversations, InstanceId me )
    {
        this.stateMachines = stateMachines;
        this.conversations = conversations;
        this.me = me;
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
            throws Throwable
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
            Class<? extends MessageType> messageType = stateMachine.getMessageType();
            MessageType typeAsEnum = (MessageType) Enum.valueOf( (Class<? extends Enum>) messageType, method.getName() );
            Message<?> message = Message.internal( typeAsEnum, arg );
            if ( me != null )
            {
                message.
                    setHeader( Message.CONVERSATION_ID, conversationId ).
                    setHeader( Message.CREATED_BY,me.toString() );
            }

            if ( method.getReturnType().equals( Void.TYPE ) )
            {
                stateMachines.process( message );
                return null;
            }
            else
            {
                ResponseFuture future = new ResponseFuture( conversationId, typeAsEnum );
                responseFutureMap.put( conversationId, future );
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
    public boolean process( Message message )
    {
        if ( !responseFutureMap.isEmpty() )
        {
            if ( !message.hasHeader( Message.TO ) )
            {
                String conversationId = message.getHeader( Message.CONVERSATION_ID );
                ResponseFuture future = responseFutureMap.get( conversationId );
                if ( future != null )
                {
                    if ( future.setPotentialResponse( message ) )
                    {
                        responseFutureMap.remove( conversationId );
                    }
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
                    Enum.valueOf( (Class<? extends Enum>) stateMachine.getMessageType(), method.getName() );

                    // Ok!
                    foundMatch = true;
                }
                catch ( Exception e )
                {
                    if ( foundMatch )
                    // State machine could only partially handle this interface
                    {
                        exception = new IllegalArgumentException( "State machine for " + stateMachine.getMessageType
                                ().getName() + " cannot handle method:" + method.getName() );
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

    class ResponseFuture
            implements Future<Object>
    {
        private final String conversationId;
        private final MessageType initiatedByMessageType;

        private Message response;

        ResponseFuture( String conversationId, MessageType initiatedByMessageType )
        {
            this.conversationId = conversationId;
            this.initiatedByMessageType = initiatedByMessageType;
        }

        public synchronized boolean setPotentialResponse( Message response )
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
            return (response.getMessageType().name().equals( initiatedByMessageType.name() + "Response" ) ||
                    response.getMessageType().name().equals( initiatedByMessageType.name() + "Failure" ));
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

            while (response == null)
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
                throw new TimeoutException();
            }
            return getResult();
        }
    }
}
