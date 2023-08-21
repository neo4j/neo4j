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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.lock.LockTracer;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.MapValue;

class ExecutingQueryFactoryTest {
    @Test
    void executingQueryWithNoTransactionBindingShouldNotExplode() {
        // GIVEN
        var factory = new ExecutingQueryFactory(
                Clocks.nanoClock(), new AtomicReference<>(CpuClock.NOT_AVAILABLE), LockTracer.NONE);
        var executingQuery = factory.createUnbound(
                "<query text>",
                MapValue.EMPTY,
                ClientConnectionInfo.EMBEDDED_CONNECTION,
                "user",
                "user",
                Collections.emptyMap());

        // WHEN
        var snapshot = executingQuery.snapshot();

        // THEN
        assertEquals(-1L, snapshot.transactionId());
    }
}
