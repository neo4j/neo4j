package org.neo4j.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.timeout.TimeoutStrategy;
import org.neo4j.kernel.logging.Logging;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.cluster.com.message.Message.internal;

public class StateMachinesTest
{
    @Test
    public void whenMessageHandlingCausesNewMessagesThenEnsureCorrectOrder() throws Exception
    {
        // Given
        StateMachines stateMachines = new StateMachines( Mockito.mock(MessageSource.class), Mockito.mock( MessageSender.class), Mockito.mock( TimeoutStrategy.class), Mockito.mock(DelayedDirectExecutor.class), new Executor()
        {
            @Override
            public void execute( Runnable command )
            {
                command.run();
            }
        } );

        ArrayList<TestMessage> handleOrder = new ArrayList<>(  );
        StateMachine stateMachine = new StateMachine( handleOrder, TestMessage.class, TestState.test, Mockito.mock(Logging.class) );

        stateMachines.addStateMachine( stateMachine );


        // When
        stateMachines.process( internal( TestMessage.message1 ) );

        // Then
        assertThat( handleOrder.toString(), equalTo( "[message1, message2, message4, message5, message3]" ) );
    }

    public enum TestMessage
            implements MessageType
    {
        message1, message2, message3, message4, message5;
    }

    public enum TestState
        implements State<List, TestMessage>
    {
        test
        {
            @Override
            public State<?, ?> handle( List context, Message<TestMessage> message, MessageHolder outgoing ) throws Throwable
            {
                context.add(message.getMessageType());

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
                }

                return this;
            }
        };
    }
}
