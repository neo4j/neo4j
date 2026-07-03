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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.neo4j.kernel.impl.locking.NoLocksClient.NO_LOCKS_CLIENT;

import org.junit.jupiter.api.Test;

class LockClientStateHolderTest {

    @Test
    void shouldAllowIncrementDecrementClientsWhileNotClosed() {
        // given
        LockClientStateHolder lockClientStateHolder = new LockClientStateHolder();

        // expect
        assertThat(lockClientStateHolder.hasActiveClients()).isFalse();
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        assertThat(lockClientStateHolder.hasActiveClients()).isTrue();
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        lockClientStateHolder.decrementActiveClients();
        lockClientStateHolder.decrementActiveClients();
        lockClientStateHolder.decrementActiveClients();
        assertThat(lockClientStateHolder.hasActiveClients()).isFalse();
    }

    @Test
    void shouldNotAllowNewClientsWhenClosed() {
        // given
        LockClientStateHolder lockClientStateHolder = new LockClientStateHolder();

        // when
        lockClientStateHolder.stopClient();

        // then
        assertThat(lockClientStateHolder.hasActiveClients()).isFalse();
        assertThatExceptionOfType(LockClientStoppedException.class)
                .isThrownBy(() -> lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT));
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
        assertThat(lockClientStateHolder.hasActiveClients()).isTrue();

        // and when
        lockClientStateHolder.stopClient();

        // expect
        assertThat(lockClientStateHolder.hasActiveClients()).isTrue();
        lockClientStateHolder.decrementActiveClients();
        assertThat(lockClientStateHolder.hasActiveClients()).isTrue();
        lockClientStateHolder.decrementActiveClients();
        assertThat(lockClientStateHolder.hasActiveClients()).isFalse();
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
        assertThat(lockClientStateHolder.hasActiveClients()).isTrue();

        // and when
        lockClientStateHolder.stopClient();

        // expect
        assertThat(lockClientStateHolder.hasActiveClients()).isTrue();
        assertThat(lockClientStateHolder.isStopped()).isTrue();

        // and when
        lockClientStateHolder.reset();

        // expect
        assertThat(lockClientStateHolder.hasActiveClients()).isFalse();
        assertThat(lockClientStateHolder.isStopped()).isFalse();

        // when
        lockClientStateHolder.incrementActiveClients(NO_LOCKS_CLIENT);
        assertThat(lockClientStateHolder.hasActiveClients()).isTrue();
        assertThat(lockClientStateHolder.isStopped()).isFalse();
    }
}
