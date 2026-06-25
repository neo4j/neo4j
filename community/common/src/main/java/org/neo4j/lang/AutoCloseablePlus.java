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
package org.neo4j.lang;

/**
 * Enriches AutoCloseable with isClosed(). This method can be used to query whether a resource was closed or
 * to make sure that it is only closed once.
 * <p>
 * Also provides ability to register a listener for when this is closed.
 */
public interface AutoCloseablePlus extends AutoCloseable {
    int UNTRACKED = -1;

    @Override
    void close();

    /**
     * Same as close(), but invoked before the listener has been notified.
     */
    void closeInternal();

    boolean isClosed();

    void setCloseListener(CloseListener closeListener);

    /**
     * Sets this resource's tracking handle: opaque bookkeeping owned by the {@code ResourcePool} that tracks it.
     * Only the owning pool may set it, and implementations must not reuse it as scratch storage. The value is
     * {@link #UNTRACKED} when not tracked by any pool.
     */
    void setTrackingHandle(int handle);

    /**
     * Returns the tracking handle set via {@link #setTrackingHandle(int)}, or {@link #UNTRACKED} if untracked.
     */
    int getTrackingHandle();
}
