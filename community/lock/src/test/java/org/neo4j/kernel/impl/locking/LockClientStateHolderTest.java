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
package org.neo4j.kernel.impl.locking;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.locking.NoLocksClient.NO_LOCKS_CLIENT;

import org.junit.jupiter.api.Test;

class LockClientStateHolderTest {

    @Test
    void shouldAllowIncrementDecrementClientsWhileNotClosed() {
        // given
        LockClientStateHolder lockClientStateHolder = new LockClientStateHolder();

        // expect
        assertFalse(lockClientStateHolder.hasActiveClients());
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        assertTrue(lockClientStateHolder.hasActiveClients());
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        lockClientStateHolder.decrementActiveClients();
        lockClientStateHolder.decrementActiveClients();
        lockClientStateHolder.decrementActiveClients();
        assertFalse(lockClientStateHolder.hasActiveClients());
    }

    @Test
    void shouldNotAllowNewClientsWhenClosed() {
        // given
        LockClientStateHolder lockClientStateHolder = new LockClientStateHolder();

        // when
        lockClientStateHolder.stopClient();

        // then
        assertFalse(lockClientStateHolder.hasActiveClients());
        assertThrows(
                LockClientStoppedException.class, () -> lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT));
    }

    @Test
    void shouldBeAbleToDecrementActiveItemAndDetectWhenFree() {
        // given
        LockClientStateHolder lockClientStateHolder = new LockClientStateHolder();

        // when
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        lockClientStateHolder.decrementActiveClients();
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);

        // expect
        assertTrue(lockClientStateHolder.hasActiveClients());

        // and when
        lockClientStateHolder.stopClient();

        // expect
        assertTrue(lockClientStateHolder.hasActiveClients());
        lockClientStateHolder.decrementActiveClients();
        assertTrue(lockClientStateHolder.hasActiveClients());
        lockClientStateHolder.decrementActiveClients();
        assertFalse(lockClientStateHolder.hasActiveClients());
    }

    @Test
    void shouldBeAbleToResetAndReuseClientState() {
        // given
        LockClientStateHolder lockClientStateHolder = new LockClientStateHolder();

        // when
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        lockClientStateHolder.decrementActiveClients();

        // expect
        assertTrue(lockClientStateHolder.hasActiveClients());

        // and when
        lockClientStateHolder.stopClient();

        // expect
        assertTrue(lockClientStateHolder.hasActiveClients());
        assertTrue(lockClientStateHolder.isStopped());

        // and when
        lockClientStateHolder.reset();

        // expect
        assertFalse(lockClientStateHolder.hasActiveClients());
        assertFalse(lockClientStateHolder.isStopped());

        // when
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        assertTrue(lockClientStateHolder.hasActiveClients());
        assertFalse(lockClientStateHolder.isStopped());
    }
}
