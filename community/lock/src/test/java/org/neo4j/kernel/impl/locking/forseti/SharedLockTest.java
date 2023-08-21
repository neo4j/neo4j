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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.lock.LockType;

class SharedLockTest {
    @Test
    void shouldUpgradeToUpdateLock() {
        // Given
        ForsetiClient clientA = mock(ForsetiClient.class);
        ForsetiClient clientB = mock(ForsetiClient.class);

        SharedLock lock = new SharedLock(clientA);
        lock.acquire(clientB);

        // When
        assertTrue(lock.tryAcquireUpdateLock());

        // Then
        assertThat(lock.numberOfHolders()).isEqualTo(2);
        assertThat(lock.isUpdateLock()).isEqualTo(true);
        assertThat(lock.type()).isEqualTo(LockType.SHARED);
    }

    @Test
    void shouldReleaseSharedLock() {
        // Given
        ForsetiClient clientA = mock(ForsetiClient.class);
        SharedLock lock = new SharedLock(clientA);

        // When
        assertTrue(lock.release(clientA));

        // Then
        assertThat(lock.numberOfHolders()).isEqualTo(0);
        assertThat(lock.isUpdateLock()).isEqualTo(false);
    }

    @Test
    void lockTypeChangesToBeExclusiveAfterUpdate() {
        var client = mock(ForsetiClient.class);

        SharedLock lock = new SharedLock(client);
        assertEquals(LockType.SHARED, lock.type());

        assertTrue(lock.tryAcquireUpdateLock());

        assertEquals(LockType.EXCLUSIVE, lock.type());
    }
}
