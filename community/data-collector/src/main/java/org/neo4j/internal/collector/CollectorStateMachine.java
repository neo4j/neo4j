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
package org.neo4j.internal.collector;

import java.util.Map;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

/**
 * Base class for managing state transitions of data-collector daemons.
 */
abstract class CollectorStateMachine<DATA> {
    private enum State {
        IDLE,
        COLLECTING
    }

    record Status(String message) {}

    record Result(boolean success, String message) {}

    static Result success(String message) {
        return new Result(true, message);
    }

    static Result error(String message) {
        return new Result(false, message);
    }

    private State state;
    private long collectionId;
    private final boolean canGetDataWhileCollecting;

    CollectorStateMachine(boolean canGetDataWhileCollecting) {
        this.canGetDataWhileCollecting = canGetDataWhileCollecting;
        state = State.IDLE;
    }

    public synchronized Status status() {
        return switch (this.state) {
            case IDLE -> new Status("idle");
            case COLLECTING -> new Status("collecting");
        };
    }

    public synchronized Result collect(Map<String, Object> config) throws InvalidArgumentsException {
        return switch (state) {
            case IDLE -> {
                state = State.COLLECTING;
                collectionId++;
                yield doCollect(config, collectionId);
            }
            case COLLECTING -> success("Collection is already ongoing.");
        };
    }

    public synchronized Result stop(long collectionIdToStop) {
        return switch (state) {
            case IDLE -> success("Collector is idle, no collection ongoing.");

            case COLLECTING -> {
                if (this.collectionId <= collectionIdToStop) {
                    state = State.IDLE;
                    yield doStop();
                }
                yield success(String.format(
                        "Collection event %d has already been stopped, a new collection event is ongoing.",
                        collectionIdToStop));
            }
        };
    }

    public synchronized Result clear() {
        return switch (state) {
            case IDLE -> doClear();
            case COLLECTING -> error("Collected data cannot be cleared while collecting.");
        };
    }

    public synchronized DATA getData() {
        return switch (state) {
            case IDLE -> doGetData();

            case COLLECTING -> {
                if (canGetDataWhileCollecting) {
                    yield doGetData();
                }
                throw new IllegalStateException("Collector is still collecting.");
            }
        };
    }

    protected abstract Result doCollect(Map<String, Object> config, long collectionId) throws InvalidArgumentsException;

    protected abstract Result doStop();

    protected abstract Result doClear();

    protected abstract DATA doGetData();
}
