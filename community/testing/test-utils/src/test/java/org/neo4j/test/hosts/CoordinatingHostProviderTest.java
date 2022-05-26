/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.test.hosts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

class CoordinatingHostProviderTest {
    @Test
    void shouldProvideUniqueHosts() {
        var hostRepository = mock(HostRepository.class);
        var hostProvider = new CoordinatingHostProvider(hostRepository);

        when(hostRepository.reserveNextHost("foo")).thenReturn(40L, 41L);
        var host1 = hostProvider.getNextFreeHost("foo");
        var host2 = hostProvider.getNextFreeHost("foo");

        assertThat(host1).isNotEqualTo(host2);
    }

    @Test
    void shouldSkipReservedHosts() {
        var hostRepository = mock(HostRepository.class);
        var hostProvider = new CoordinatingHostProvider(hostRepository);

        when(hostRepository.reserveNextHost("foo")).thenReturn(40L, 41L, 43L);
        assertThat(hostProvider.getNextFreeHost("foo")).isEqualTo(40L);
        assertThat(hostProvider.getNextFreeHost("foo")).isEqualTo(41L);
        assertThat(hostProvider.getNextFreeHost("foo")).isEqualTo(43L);
    }
}
