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

import static org.neo4j.values.utils.TemporalUtil.NANOS_PER_SECOND;

import java.time.Duration;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Configuration of transaction timeout.
 * <p>
 * In case if timeout is {@link Duration#ZERO} - transaction does not have a timeout.
 *
 * @param timeoutNanos max transaction duration in nanos since its start.
 * @param status status to use if the transaction actually times out.
 *                     It should be either {@link Status.Transaction#TransactionTimedOut}
 *                     or {@link Status.Transaction#TransactionTimedOutClientConfiguration}.
 *                     Which one depends on how the timeout was configured.
 */
public record TransactionTimeout(long timeoutNanos, Status status) {

    // duration may include a nanoseconds part added after converting from seconds, so subtract one second for that case
    private static final long SAFE_SECOND_BOUNDARY = (Long.MAX_VALUE / NANOS_PER_SECOND) - 1;

    public TransactionTimeout(Duration duration, Status status) {
        this(convertToNanos(duration), status);
    }

    private static long convertToNanos(Duration timeout) {
        if (timeout.getSeconds() < SAFE_SECOND_BOUNDARY) {
            return timeout.toNanos();
        }
        return Long.MAX_VALUE;
    }

    public static final TransactionTimeout NO_TIMEOUT =
            new TransactionTimeout(Duration.ZERO, Status.Transaction.TransactionTimedOut);
}
