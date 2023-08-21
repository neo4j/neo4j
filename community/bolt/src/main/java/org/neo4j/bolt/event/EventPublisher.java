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
package org.neo4j.bolt.event;

import java.util.function.Consumer;
import org.neo4j.function.ThrowingConsumer;

public interface EventPublisher<L> {

    /**
     * Invokes a given event function on all registered listeners within this publisher.
     *
     * @param eventFunction an event function.
     */
    void dispatch(Consumer<L> eventFunction);

    /**
     * Invokes a given event function on all registered listeners within this publisher.
     *
     * @param eventFunction an event function.
     * @param <E> an acceptable exception.
     */
    <E extends Throwable> void dispatchThrowing(ThrowingConsumer<L, E> eventFunction) throws E;

    /**
     * Invokes a given event function on all registered listeners within this publisher ignoring any
     * exceptions thrown as a result.
     *
     * @param eventFunction an event function.
     */
    void dispatchSafe(ThrowingConsumer<L, Exception> eventFunction);

    /**
     * Subscribes a given listener to the events published by this publisher object.
     *
     * @param listener a listener implementation.
     */
    void registerListener(L listener);

    /**
     * Un-subscribes a given listener from the events published by this publisher object.
     *
     * @param listener a listener implementation.
     */
    void removeListener(L listener);
}
