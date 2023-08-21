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
package org.neo4j.kernel.api;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * Used to track information about a transaction's termination state.
 */
public class TerminationMark {
    private final Status reason;
    private final long timestampNanos;
    private volatile boolean stale;

    public TerminationMark(Status reason, long timestampNanos) {
        this.reason = reason;
        this.timestampNanos = timestampNanos;
        this.stale = false;
    }

    public boolean isMarkedAsStale() {
        return stale;
    }

    /**
     * Used by transaction monitor job when a transaction has been marked for termination a long time.
     */
    public void markAsStale() {
        stale = true;
    }

    public Status getReason() {
        return reason;
    }

    public long getTimestampNanos() {
        return timestampNanos;
    }
}
