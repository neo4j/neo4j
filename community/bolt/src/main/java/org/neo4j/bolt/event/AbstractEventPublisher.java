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

import java.util.List;
import java.util.function.Consumer;
import org.neo4j.function.ThrowingConsumer;

public class AbstractEventPublisher<L> implements EventPublisher<L> {
    private final List<L> listeners;

    protected AbstractEventPublisher(List<L> listeners) {
        this.listeners = listeners;
    }

    @Override
    public void dispatch(Consumer<L> eventFunction) {
        this.listeners.forEach(listener -> {
            try {
                eventFunction.accept(listener);
            } catch (Exception ignore) {
            }
        });
    }

    @Override
    public void dispatchSafe(ThrowingConsumer<L, Exception> eventFunction) {
        this.listeners.forEach(listener -> {
            try {
                eventFunction.accept(listener);
            } catch (Exception ignore) {
            }
        });
    }

    @Override
    public <E extends Throwable> void dispatchThrowing(ThrowingConsumer<L, E> eventFunction) throws E {
        for (var listener : this.listeners) {
            eventFunction.accept(listener);
        }
    }

    @Override
    public void registerListener(L listener) {
        if (this.listeners.contains(listener)) {
            return;
        }

        this.listeners.add(listener);
    }

    @Override
    public void removeListener(L listener) {
        this.listeners.remove(listener);
    }
}
