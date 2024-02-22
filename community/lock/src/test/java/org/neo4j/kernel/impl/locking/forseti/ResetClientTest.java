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
package org.neo4j.kernel.impl.locking.forseti;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.time.Clocks;

public class ResetClientTest {
    @Test
    void resetLockClientTest() {
        Config config = Config.defaults();
        ForsetiLockManager lockManager = new ForsetiLockManager(config, Clocks.nanoClock(), ResourceType.values());
        LockManager.Client client = lockManager.newClient();
        client.initialize(LeaseService.NoLeaseClient.INSTANCE, 1, EmptyMemoryTracker.INSTANCE, config);

        client.acquireExclusive(LockTracer.NONE, ResourceType.NODE, 1);
        assertEquals(1, client.activeLockCount());

        client.reset();
        assertEquals(0, client.activeLockCount());

        client.acquireExclusive(LockTracer.NONE, ResourceType.NODE, 1);
        client.acquireExclusive(LockTracer.NONE, ResourceType.NODE, 2);
        client.acquireExclusive(LockTracer.NONE, ResourceType.NODE, 3);
        assertEquals(3, client.activeLockCount());
    }
}
