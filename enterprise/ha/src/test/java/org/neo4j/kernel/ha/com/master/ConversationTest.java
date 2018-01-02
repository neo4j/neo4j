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
package org.neo4j.kernel.ha.com.master;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Function;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.test.ThreadingRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith( MockitoJUnitRunner.class )
public class ConversationTest
{
    @Mock
    private Locks.Client client;
    @InjectMocks
    private Conversation conversation;
    @Rule
    public ThreadingRule threadingRule = new ThreadingRule();

    @Test
    public void stopAlreadyClosedConversationDoNotTouchLocks()
    {
        conversation.close();
        conversation.stop();
        conversation.stop();
        conversation.stop();

        verify( client ).close();
        assertFalse( conversation.isActive() );
        verifyNoMoreInteractions( client );
    }

    @Test
    public void stopCloseConversation()
    {
        conversation.stop();
        conversation.close();

        verify( client ).stop();
        verify( client ).close();
        assertFalse( conversation.isActive() );
    }

    @Test(timeout = 3000)
    public void conversationCanNotBeStoppedAndClosedConcurrently() throws InterruptedException
    {
        final CountDownLatch answerLatch = new CountDownLatch( 1 );
        final CountDownLatch stopLatch = new CountDownLatch( 1 );
        final CountDownLatch stopReadyLatch = new CountDownLatch( 1 );
        final int sleepTime = 1000;
        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                stopReadyLatch.countDown();
                stopLatch.await();
                TimeUnit.MILLISECONDS.sleep( sleepTime );
                return null;
            }
        } ).when( client ).stop();
        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                answerLatch.countDown();
                return null;
            }
        } ).when( client ).close();

        threadingRule.execute( stopConversation(), conversation );

        stopReadyLatch.await();
        threadingRule.execute( closeConversation(), conversation );

        long raceStartTime = System.currentTimeMillis();
        stopLatch.countDown();
        answerLatch.await();
        // execution time should be at least 1000 millis
        long executionTime = System.currentTimeMillis() - raceStartTime;
        assertTrue(String.format( "Execution time should be at least equal to %d, but was %d.", sleepTime, executionTime),
                executionTime  >= sleepTime);
    }

    private Function<Conversation,Void> closeConversation()
    {
        return new Function<Conversation,Void>()
        {
            @Override
            public Void apply( Conversation conversation )
            {
                conversation.close();
                return null;
            }
        };
    }

    private Function<Conversation,Void> stopConversation()
    {
        return new Function<Conversation,Void>()
        {
            @Override
            public Void apply( Conversation conversation )
            {
                conversation.stop();
                return null;
            }
        };
    }
}