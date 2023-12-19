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
package org.neo4j.cluster;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.cluster.com.message.Message.internal;

public class StateMachinesTest
{
    @Test
    public void whenMessageHandlingCausesNewMessagesThenEnsureCorrectOrder()
    {
        // Given
        StateMachines stateMachines = new StateMachines( NullLogProvider.getInstance(), mock( StateMachines.Monitor.class ),
                mock( MessageSource.class ),
                Mockito.mock( MessageSender.class ), Mockito.mock( Timeouts.class ),
                Mockito.mock( DelayedDirectExecutor.class ), Runnable::run, mock( InstanceId.class )
        );

        ArrayList<TestMessage> handleOrder = new ArrayList<>();
        StateMachine stateMachine = new StateMachine( handleOrder, TestMessage.class, TestState.test, NullLogProvider.getInstance() );

        stateMachines.addStateMachine( stateMachine );

        // When
        stateMachines.process( internal( TestMessage.message1 ) );

        // Then
        assertThat( handleOrder.toString(), equalTo( "[message1, message2, message4, message5, message3]" ) );
    }

    @Test
    public void shouldAlwaysAddItsInstanceIdToOutgoingMessages()
    {
        InstanceId me = new InstanceId( 42 );
        final List<Message> sentOut = new LinkedList<>();

        /*
         * Lots of setup required. Must have a sender that keeps messages so we can see what the machine sent out.
         * We must have the StateMachines actually delegate the incoming message and retrieve the generated outgoing.
         * That means we need an actual StateMachine with a registered MessageType. And most of those are void
         * methods, which means lots of Answer objects.
         */
        // Given
        MessageSender sender = mock( MessageSender.class );
        // The sender, which adds messages outgoing to the list above.
        doAnswer( invocation ->
        {
            sentOut.addAll( invocation.getArgument( 0 ) );
            return null;
        } ).when( sender ).process( ArgumentMatchers.<List<Message<? extends MessageType>>>any() );

        StateMachines stateMachines = new StateMachines( NullLogProvider.getInstance(), mock( StateMachines.Monitor.class ),
                mock( MessageSource.class ), sender,
                mock( Timeouts.class ), mock( DelayedDirectExecutor.class ), Runnable::run, me
        );

        // The state machine, which has a TestMessage message type and simply adds a HEADER_TO header to the messages it
        // is handed to handle.
        StateMachine machine = mock( StateMachine.class );
        when( machine.getMessageType() ).then( (Answer<Object>) invocation -> TestMessage.class );
        doAnswer( invocation ->
        {
            Message message = invocation.getArgument( 0 );
            MessageHolder holder = invocation.getArgument( 1 );
            message.setHeader( Message.HEADER_TO, "to://neverland" );
            holder.offer( message );
            return null;
        } ).when( machine ).handle( any( Message.class ), any( MessageHolder.class ) );
        stateMachines.addStateMachine( machine );

        // When
        stateMachines.process( Message.internal( TestMessage.message1 ) );

        // Then
        assertEquals( "StateMachines should not make up messages from thin air", 1, sentOut.size() );
        Message sent = sentOut.get( 0 );
        assertTrue( "StateMachines should add the instance-id header", sent.hasHeader( Message.HEADER_INSTANCE_ID ) );
        assertEquals( "StateMachines should add instance-id header that has the correct value",
                me.toString(), sent.getHeader( Message.HEADER_INSTANCE_ID ) );
    }

    public enum TestMessage
            implements MessageType
    {
        message1, message2, message3, message4, message5
    }

    public enum TestState
            implements State<List, TestMessage>
    {
        test
                {
                    @Override
                    public TestState handle( List context, Message<TestMessage> message,
                                               MessageHolder outgoing )
                    {
                        context.add( message.getMessageType() );

                        switch ( message.getMessageType() )
                        {
                            case message1:
                            {
                                outgoing.offer( internal( TestMessage.message2 ) );
                                outgoing.offer( internal( TestMessage.message3 ) );
                                break;
                            }

                            case message2:
                            {
                                outgoing.offer( internal( TestMessage.message4 ) );
                                outgoing.offer( internal( TestMessage.message5 ) );
                                break;
                            }

                            default:
                                break;
                        }

                        return this;
                    }
                }
    }
}
