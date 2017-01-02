/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.schedule;

/**
 * A service for creating {@link RenewableTimeout} instances. Implementations of this interface are expected
 * but not required to also act as lifecycle managers for the timeouts they return.
 */
public interface RenewableTimeoutService
{
    /**
     * The main factory method. Returns a {@link RenewableTimeout} handle for a {@link TimeoutHandler} that will trigger once,
     * after {@param delayInMillis} plus or minus a random interval, unless renewed. Upon triggering, the supplied
     * {@param handler} will be called.
     * <br/>
     * The time delay is a best effort service. In essence, the delay should not be expected to be accurate to the
     * millisecond, as thread scheduling and other factors may influence when the handler is actually called.
     *
     * @param timeoutName  The timeout name, for lookup purposes
     * @param delayInMillis The amount of time after this timeout will be triggered
     * @param randomRangeInMillis  The upper limit of a range of longs (0 is the lower limit) from which a random value will be
     *                     selected with uniform probability, and added to the delay. Setting this value to 0 means
     *                     no randomness.
     * @param handler      The {@link TimeoutHandler} to call when this timeout triggers
     * @return The timeout handle
     */
    RenewableTimeout create( TimeoutName timeoutName, long delayInMillis, long randomRangeInMillis, TimeoutHandler handler );

    interface TimeoutName
    {
        String name();
    }

    /**
     * The handle for a timeout.
     * This represents the timeout for calling a {@link TimeoutHandler}, as it is returned from a call to
     * {@link RenewableTimeoutService#create(TimeoutName, long, long, TimeoutHandler)}.
     */
    interface RenewableTimeout
    {
        /**
         * Renews the timeout for the handler represented by this instance. The effect of calling this method is that
         * upon return, the handler will be called in an interval equal to that provided to the
         * {@link RenewableTimeoutService#create(TimeoutName, long, long, TimeoutHandler)} on creation.
         * <br/>
         * This timeout renewal effect takes place regardless of whether the handler has been called or not or if it
         * has been cancelled.
         */
        void renew();

        /**
         * Cancels this timeout. If the handler has not been called, it will not be called when the delay specified on
         * creation elapses. Calling {@link #renew()} will reset the delay and the handler will eventually be called.
         */
        void cancel();
    }

    /**
     * Represents the action to take upon expiry of a timeout.
     */
    interface TimeoutHandler
    {
        /**
         * The callback method. This method is expected to execute in non blocking fashion, since it's possible that
         * it will be run from the timer thread that will call other handlers as well.
         * @param timeout The timeout representing this handler. This is mainly provided so that the timeout can be
         *                renewed upon completion of the callback.
         */
        void onTimeout( RenewableTimeout timeout );
    }
}
