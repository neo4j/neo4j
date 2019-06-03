/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.test.extension.actors;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.extension.Inject;

import static java.time.Duration.ofMinutes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class ActorsSupportExtensionTest
{
    @Nested
    @ActorsExtension
    class InjectOneField
    {
        @Inject
        Actor actor;

        @Test
        void actorMustBeInjected()
        {
            assertNotNull( actor );
        }

        @Test
        void actorMustRunSubmittedTasks()
        {
            assertTimeoutPreemptively( ofMinutes( 1 ), () ->
            {
                CountDownLatch l1 = new CountDownLatch( 1 );
                Future<Void> f1 = actor.submit( l1::countDown );
                l1.await();
                assertNull( f1.get() );

                CountDownLatch l2 = new CountDownLatch( 1 );
                Future<String> f2 = actor.submit( l2::countDown, "bla" );
                l2.await();
                assertEquals( f2.get(), "bla" );

                CountDownLatch l3 = new CountDownLatch( 1 );
                Future<String> f3 = actor.submit( () ->
                {
                    l3.countDown();
                    return "bla";
                } );
                l3.await();
                assertEquals( f3.get(), "bla" );
            } );
        }

        @Test
        void mustBeAbleToObserveTimedWaiting()
        {
            assertTimeoutPreemptively( ofMinutes( 1 ), () ->
            {
                CountDownLatch latch = new CountDownLatch( 1 );
                Future<?> future = actor.submit( () ->
                {
                    latch.await();
                    return null;
                } );
                actor.untilWaitingIn( CountDownLatch.class.getMethod( "await" ) );
                latch.countDown();
                future.get();
            } );
        }

        @Test
        void untilMethodsMustThrowIfActorIsNotStarted()
        {
            assertThrows( IllegalStateException.class, () -> actor.untilWaiting() );
        }

        @Test
        void untilMethodsMustThrowIfActorIsStopped() throws Exception
        {
            actor.submit( () -> {} ).get(); // Ensure that the actor has started.
            ActorImpl actorImpl = (ActorImpl) actor;
            actorImpl.stop();
            actorImpl.join();
            assertThrows( AssertionError.class, () -> actor.untilWaiting() );
        }

        @Test
        void submitMethodsMustThrowIfActorIsStopped() throws Exception
        {
            actor.submit( () -> {} ).get(); // Ensure that the actor has started.
            ActorImpl actorImpl = (ActorImpl) actor;
            actorImpl.stop();
            actorImpl.join();
            assertThrows( IllegalStateException.class, () -> actor.submit( () -> {} ) );
        }

        @Test
        void untilMethodsMustThrowIfActorIsIdle() throws Exception
        {
            actor.submit( () -> {} ).get(); // Ensure that the actor has started.
            // Because nothing is running, and no tasks are queued up, so there is nothing to wait for.
            assertThrows( IllegalStateException.class, () -> actor.untilWaiting() );
        }

        @Test
        void mustBeAbleToInterruptActors() throws Exception
        {
            CountDownLatch l1 = new CountDownLatch( 1 );
            Future<?> f1 = actor.submit( () ->
            {
                l1.await();
                return null;
            } );
            actor.untilWaitingIn( CountDownLatch.class.getMethod( "await" ) );
            actor.interrupt();
            ExecutionException ee = assertThrows( ExecutionException.class, f1::get );
            assertThat( ee.getCause(), instanceOf( InterruptedException.class ) );
        }

        @Test
        void mustBeAbleToInterruptUntilMethods()
        {
            Object lock = new Object();
            synchronized ( lock )
            {
                actor.submit( () ->
                {
                    synchronized ( lock )
                    {
                        return null;
                    }
                } );

                Thread.currentThread().interrupt();
                // The actor will not be waiting. It will be in BLOCKED state, because that's how 'synchronized' works.
                assertThrows( InterruptedException.class, actor::untilWaiting );
            }
        }
    }

    @Nested
    @ActorsExtension
    class InjectTwoFields
    {
        @Inject
        Actor emil;
        @Inject
        Actor jim;

        @Test
        void actorsMustBeDifferent()
        {
            assertNotNull( emil );
            assertNotNull( jim );
            assertNotSame( emil, jim );
        }

        @Test
        void actorsMustBeIndependent()
        {
            assertTimeoutPreemptively( ofMinutes( 1 ), () ->
            {
                CountDownLatch l1 = new CountDownLatch( 1 );
                CountDownLatch l2 = new CountDownLatch( 1 );
                Future<?> f1 = emil.submit( () ->
                {
                    l1.await();
                    return null;
                } );
                Future<?> f2 = jim.submit( () ->
                {
                    l2.await();
                    return null;
                } );
                emil.untilWaitingIn( CountDownLatch.class.getMethod( "await" ) );
                jim.untilWaitingIn( CountDownLatch.class.getMethod( "await" ) );
                l1.countDown();
                l2.countDown();
                f1.get();
                f2.get();
            } );
        }
    }

    @Nested
    @ActorsExtension
    class NestingTestOuter
    {
        @Inject
        Actor outerActor;

        @Nested
        @ActorsExtension
        class Middle
        {
            @Inject
            Actor middleActor;

            @Nested
            @ActorsExtension
            class Inner
            {
                @Inject
                Actor innerActor;

                @Test
                void nestingTest() throws Exception
                {
                    AtomicInteger counter = new AtomicInteger();
                    Future<Integer> f1 = innerActor.submit( counter::incrementAndGet );
                    Future<Integer> f2 = middleActor.submit( counter::incrementAndGet );
                    Future<Integer> f3 = outerActor.submit( counter::incrementAndGet );
                    f1.get();
                    f2.get();
                    f3.get();
                    assertEquals( counter.get(), 3 );
                }
            }

            @Test
            void nestingTest() throws Exception
            {
                AtomicInteger counter = new AtomicInteger();
                Future<Integer> f1 = middleActor.submit( counter::incrementAndGet );
                Future<Integer> f2 = outerActor.submit( counter::incrementAndGet );
                f1.get();
                f2.get();
                assertEquals( counter.get(), 2 );
            }
        }

        @Test
        void nestingTest() throws Exception
        {
            AtomicInteger counter = new AtomicInteger();
            Future<Integer> f1 = outerActor.submit( counter::incrementAndGet );
            f1.get();
            assertEquals( counter.get(), 1 );
        }
    }
}
