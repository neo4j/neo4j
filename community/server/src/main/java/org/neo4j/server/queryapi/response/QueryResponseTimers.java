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
package org.neo4j.server.queryapi.response;

import java.time.Duration;
import java.time.Instant;

/**
 * Stop watch used to measure query times such as time take to result get available and consumed.
 */
public class QueryResponseTimers {

    private Long resultAvailableAfter;
    private Long resultConsumedAfter;
    private final Instant startInstant;

    private QueryResponseTimers(Instant startInstant) {
        this.startInstant = startInstant;
    }

    /**
     * Starts a new time using the System clock
     * @return The timer
     */
    public static QueryResponseTimers start() {
        return new QueryResponseTimers(Instant.now());
    }

    /**
     * Notifies the result is available.
     * <p/>
     * Only captures the first all. Following calls will be ignored.
     */
    public void notifyResultAvailable() {
        if (resultAvailableAfter == null) {
            this.resultAvailableAfter =
                    Duration.between(startInstant, Instant.now()).toMillis();
        }
    }

    /**
     * Notifies the result is consumed.
     * <p/>
     * Only captures the first all. Following calls will be ignored.
     */
    public void notifyResultConsumed() {
        if (resultConsumedAfter == null) {
            this.resultConsumedAfter =
                    Duration.between(startInstant, Instant.now()).toMillis();
        }
    }

    /**
     * @return result available after in milliseconds. Null, if {@link #notifyResultAvailable()} is not called.
     */
    public Long resultAvailableAfter() {
        return resultAvailableAfter;
    }

    /**
     * @return result available after in milliseconds. Null, if {@link #notifyResultConsumed()} is not called.
     */
    public Long resultConsumedAfter() {
        return resultConsumedAfter;
    }
}
