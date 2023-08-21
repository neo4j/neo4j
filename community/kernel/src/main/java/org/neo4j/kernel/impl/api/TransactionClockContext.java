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
package org.neo4j.kernel.impl.api;

import static java.time.Instant.ofEpochMilli;

import java.time.Clock;
import java.time.ZoneId;
import java.util.Objects;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

public final class TransactionClockContext implements ClockContext {
    private final SystemNanoClock system;
    private final ZoneId timezone;
    private Clock statement;
    private Clock transaction;

    public TransactionClockContext() {
        this(Clocks.nanoClock());
    }

    public TransactionClockContext(SystemNanoClock clock) {
        this.system = Objects.requireNonNull(clock, "system clock");
        this.timezone = clock.getZone();
    }

    void initializeTransaction(long transactionStartTimeMillis) {
        this.transaction = Clock.fixed(ofEpochMilli(transactionStartTimeMillis), timezone);
        this.statement = null;
    }

    void initializeStatement() {
        if (this.statement == null) // this is the first statement in the transaction, use the transaction time
        {
            this.statement = this.transaction;
        } else // this is not the first statement in the transaction, initialize with a new time
        {
            this.statement = Clock.fixed(system.instant(), timezone);
        }
    }

    @Override
    public SystemNanoClock systemClock() {
        return system;
    }

    @Override
    public Clock statementClock() {
        assert statement != null : "statement clock not initialized";
        return statement;
    }

    @Override
    public Clock transactionClock() {
        assert transaction != null : "transaction clock not initialized";
        return transaction;
    }
}
