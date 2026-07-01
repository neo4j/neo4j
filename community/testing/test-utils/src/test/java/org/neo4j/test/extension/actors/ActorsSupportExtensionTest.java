/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.test.extension.actors;

import static java.time.Duration.ofMinutes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.lang.reflect.Executable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.test.extension.Inject;

class ActorsSupportExtensionTest {
    @Nested
    @ActorsExtension
    class InjectOneField {
        @Inject
        Actor actor;

        @Test
        void actorMustBeInjected() {
            assertThat(actor).isNotNull();
        }

        @Test
        void actorMustRunSubmittedTasks() {
            assertTimeoutPreemptively(ofMinutes(1), () -> {
                CountDownLatch l1 = new CountDownLatch(1);
                Future<Void> f1 = actor.submit(l1::countDown);
                l1.await();
                assertThat(f1.get()).isNull();

                CountDownLatch l2 = new CountDownLatch(1);
                Future<String> f2 = actor.submit(l2::countDown, "bla");
                l2.await();
                assertThat(f2.get()).isEqualTo("bla");

                CountDownLatch l3 = new CountDownLatch(1);
                Future<String> f3 = actor.submit(() -> {
                    l3.countDown();
                    return "bla";
                });
                l3.await();
                assertThat(f3.get()).isEqualTo("bla");
            });
        }

        @Test
        void mustBeAbleToObserveTimedWaiting() {
            assertTimeoutPreemptively(ofMinutes(1), () -> {
                CountDownLatch latch = new CountDownLatch(1);
                Future<?> future = actor.submit(() -> {
                    latch.await();
                    return null;
                });
                actor.untilWaitingIn(CountDownLatch.class.getMethod("await"));
                latch.countDown();
                future.get();
            });
        }

        @Test
        void untilMethodsMustThrowIfActorIsNotStarted() {
            assertThatThrownBy(() -> actor.untilWaiting()).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void untilMethodsMustThrowIfActorIsStopped() throws Exception {
            actor.submit(() -> {}).get(); // Ensure that the actor has started.
            ActorImpl actorImpl = (ActorImpl) actor;
            actorImpl.stop();
            actorImpl.join();
            assertThatThrownBy(() -> actor.untilWaiting()).isInstanceOf(AssertionError.class);
        }

        @Test
        void submitMethodsMustThrowIfActorIsStopped() throws Exception {
            actor.submit(() -> {}).get(); // Ensure that the actor has started.
            ActorImpl actorImpl = (ActorImpl) actor;
            actorImpl.stop();
            actorImpl.join();
            assertThatThrownBy(() -> actor.submit(() -> {})).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void untilMethodsMustThrowIfActorIsIdle() throws Exception {
            actor.submit(() -> {}).get(); // Ensure that the actor has started.
            // Because nothing is running, and no tasks are queued up, so there is nothing to wait for.
            assertThatThrownBy(() -> actor.untilWaiting()).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void mustBeAbleToInterruptActors() throws Exception {
            CountDownLatch l1 = new CountDownLatch(1);
            Future<?> f1 = actor.submit(() -> {
                l1.await();
                return null;
            });
            actor.untilWaitingIn(CountDownLatch.class.getMethod("await"));
            actor.interrupt();
            assertThatThrownBy(f1::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
        }

        @Test
        void mustBeAbleToInterruptUntilMethods() {
            Object lock = new Object();
            synchronized (lock) {
                actor.submit(() -> {
                    synchronized (lock) {
                        return null;
                    }
                });

                Thread.currentThread().interrupt();
                // The actor will not be waiting. It will be in BLOCKED state, because that's how 'synchronized' works.
                assertThatThrownBy(actor::untilWaiting).isInstanceOf(InterruptedException.class);
            }
        }

        /**
         * This is the example code used in the javadoc for {@link Actor#untilWaitingIn(Executable)}.
         */
        @Test
        void example() throws Exception {
            actor.submit(new Sleeper()::sleep);
            actor.untilWaitingIn(Sleeper.class.getMethod("sleep"));
            actor.interrupt();
        }

        static class Sleeper {
            public void sleep() {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    @Nested
    @ActorsExtension
    class InjectTwoFields {
        @Inject
        Actor emil;

        @Inject
        Actor jim;

        @Test
        void actorsMustBeDifferent() {
            assertThat(emil).isNotNull();
            assertThat(jim).isNotNull();
            assertThat(emil).isNotSameAs(jim);
        }

        @Test
        void actorsMustBeIndependent() {
            assertTimeoutPreemptively(ofMinutes(1), () -> {
                CountDownLatch l1 = new CountDownLatch(1);
                CountDownLatch l2 = new CountDownLatch(1);
                Future<?> f1 = emil.submit(() -> {
                    l1.await();
                    return null;
                });
                Future<?> f2 = jim.submit(() -> {
                    l2.await();
                    return null;
                });
                emil.untilWaitingIn(CountDownLatch.class.getMethod("await"));
                jim.untilWaitingIn(CountDownLatch.class.getMethod("await"));
                l1.countDown();
                l2.countDown();
                f1.get();
                f2.get();
            });
        }
    }

    @Nested
    @ActorsExtension
    class NestingTestOuter {
        @Inject
        Actor outerActor;

        @Nested
        @ActorsExtension
        class Middle {
            @Inject
            Actor middleActor;

            @Nested
            @ActorsExtension
            class Inner {
                @Inject
                Actor innerActor;

                @Test
                void nestingTest() throws Exception {
                    AtomicInteger counter = new AtomicInteger();
                    Future<Integer> f1 = innerActor.submit(counter::incrementAndGet);
                    Future<Integer> f2 = middleActor.submit(counter::incrementAndGet);
                    Future<Integer> f3 = outerActor.submit(counter::incrementAndGet);
                    f1.get();
                    f2.get();
                    f3.get();
                    assertThat(counter.get()).isEqualTo(3);
                }
            }

            @Test
            void nestingTest() throws Exception {
                AtomicInteger counter = new AtomicInteger();
                Future<Integer> f1 = middleActor.submit(counter::incrementAndGet);
                Future<Integer> f2 = outerActor.submit(counter::incrementAndGet);
                f1.get();
                f2.get();
                assertThat(counter.get()).isEqualTo(2);
            }
        }

        @Test
        void nestingTest() throws Exception {
            AtomicInteger counter = new AtomicInteger();
            Future<Integer> f1 = outerActor.submit(counter::incrementAndGet);
            f1.get();
            assertThat(counter.get()).isEqualTo(1);
        }
    }

    static IntStream parameters() {
        return IntStream.range(1, 10);
    }

    enum ParametersEnum {
        A,
        B
    }

    @Nested
    @ActorsExtension
    class ActorsAndParameterisedTests {
        @Inject
        Actor actor;

        @ParameterizedTest
        @MethodSource("org.neo4j.test.extension.actors.ActorsSupportExtensionTest#parameters")
        void methodSourcedParameterisedTestWithActors(int ignored) throws Exception {
            actor.submit(() -> {}).get();
        }

        @ParameterizedTest
        @EnumSource(ParametersEnum.class)
        void enumSourcedParameterisedTestWithActors(ParametersEnum ignored) throws Exception {
            actor.submit(() -> {}).get();
        }
    }

    @Nested
    class ActorsAndParameterisedTestTemplates extends ActorsAndParameterisedTests {
        // Running tests inherited from the super class.
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ActorsAndParameterisedTestWithPerClassLifecycle extends ActorsAndParameterisedTests {
        // Running tests inherited from the super class.
    }
}
