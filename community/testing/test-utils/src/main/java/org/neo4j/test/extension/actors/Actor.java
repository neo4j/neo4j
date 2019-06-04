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

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.neo4j.test.extension.Inject;

/**
 * An independent test actor that runs in its own thread.
 * <p>
 * Tasks can be submitted to this thread and be executed in order, and its state can be inspected.
 * <p>
 * Instances of this interface are acquired by having them {@link Inject injected} into instance fields of test classes.
 * This requires that the test class are running with the {@link ActorsExtension}.
 */
public interface Actor
{
    /**
     * Submit the given task to be executed by this actor.
     *
     * @param callable The callable to be executed.
     * @param <T> The return type of the callable.
     * @return A future that represents the asynchronous execution of the given callable.
     */
    <T> Future<T> submit( Callable<T> callable );

    /**
     * Submit the given task to be executed by this actor.
     *
     * @param runnable The runnable to be executed.
     * @param result The value returned if the task completed successfully.
     * @param <T> The type of the return value.
     * @return A future that represents the asynchronous execution of the given runnable.
     */
    <T> Future<T> submit( Runnable runnable, T result );

    /**
     * Submit the given task to be executed.
     *
     * @param runnable The runnable to be executed.
     * @return A future of void that represents the asynchronous execution of the given runnable.
     */
    Future<Void> submit( Runnable runnable );

    /**
     * Wait for the actor thread to enter a waiting state.
     * <p>
     * A waiting state means that the actor thread state is one of {@link Thread.State#WAITING} or {@link Thread.State#TIMED_WAITING}.
     * <p>
     * If there are no tasks running or queued, then an {@link IllegalStateException} will be thrown to prevent the actor and the calling thread from
     * live-locking.
     * <p>
     * If the actor thread has terminated (which only happens <em>after</em> the test method has returned) then an {@link AssertionError} is thrown.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting for the actor to enter a waiting state.
     */
    void untilWaiting() throws InterruptedException;

    /**
     * Waits for the actor thread to enter {@link #untilWaiting() a waiting state} inside the given method or constructor.
     * <p>
     * Here is an example of how this can be used:
     * <pre><code>
     * &#64;ActorsExtension
     * class ExampleTest
     * {
     *    &#64;Inject
     *    Actor actor;
     *
     *    &#64;Test
     *    void example() throws Exception
     *    {
     *        actor.submit( new Sleeper()::sleep );
     *        actor.untilWaitingIn( Sleeper.class.getMethod( "sleep" ) );
     *        actor.interrupt();
     *    }
     *
     *    class Sleeper
     *    {
     *        public void sleep()
     *        {
     *            try
     *            {
     *                Thread.sleep( 1_000 );
     *            }
     *            catch ( InterruptedException ignore )
     *            {
     *            }
     *        }
     *    }
     * }
     * </code></pre>
     *
     * @param constructorOrMethod The {@link Method} or {@link Constructor} that the actor should be waiting in.
     * @throws InterruptedException If the calling thread is interrupted while waiting for the actor thread to reach the intended state.
     */
    void untilWaitingIn( Executable constructorOrMethod ) throws InterruptedException;

    /**
     * Waits for the actor thread to enter {@link #untilWaiting() a waiting state} inside any method with the given name.
     * <p>
     * This is similar to {@link #untilWaitingIn(Executable)}, but can also be used for when the target class cannot be referenced from the test code.
     * For instance, if the class is anonymous, or not public.
     *
     * @param methodName The name of the method wherein the actor should be waiting.
     * @throws InterruptedException If the calling thread is interrupted while waiting for the actor thread to reach the intended state.
     */
    void untilWaitingIn( String methodName ) throws InterruptedException;

    /**
     * Waits for the actor thread to enter any of the given states.
     * <p>
     * Note that unlike {@link #untilWaiting()}, this method will <em>not</em> throw any exception if the actor has no submitted tasks, or is terminated.
     * This means that it is possible to live-lock with the actor thread.
     *
     * @param states The set of states to wait for.
     */
    void untilThreadState( Thread.State... states );

    /**
     * Interrupt the actor thread.
     */
    void interrupt();
}
