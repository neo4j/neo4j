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
package org.neo4j.function;

import java.io.Closeable;

/**
 * An interface for reactive programming whereby a consumer can subscribe to changes to a value over time
 */
public interface Observable<T> {
    /**
     * Synchronous access to the latest value
     *
     * @return The current value
     */
    T latestValue();

    /**
     * Subscribe to updates to the value. Note that the Observable will retain a reference to the Observer, meaning the
     * Observer will not be garbage collected while the Observable still lives.
     *
     * @param observer A subscriber that will be called when the value changes
     * @return A reference to the subscription, which should unsubscribe from the Observable when closed
     */
    Closeable subscribe(Observer<T> observer);
}
