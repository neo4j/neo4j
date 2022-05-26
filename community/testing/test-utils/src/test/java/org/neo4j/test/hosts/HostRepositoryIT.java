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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.test.hosts.HostConstants.EPHEMERAL_HOST_MAXIMUM;
import static org.neo4j.test.hosts.HostConstants.EPHEMERAL_HOST_MINIMUM;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HostRepositoryIT {
    @Test
    void shouldReserveHosts() throws Exception {
        var hostRepository1 = new HostRepository(temporaryDirectory(), EPHEMERAL_HOST_MINIMUM);

        var host1 = hostRepository1.reserveNextHost("foo");
        var host2 = hostRepository1.reserveNextHost("foo");
        var host3 = hostRepository1.reserveNextHost("foo");

        assertThat(Set.of(host1, host2, host3)).hasSize(3);
    }

    @Test
    void shouldCoordinateUsingFileSystem() throws Exception {
        var temporaryDirectory = temporaryDirectory();
        var hostRepository1 = new HostRepository(temporaryDirectory, EPHEMERAL_HOST_MINIMUM);
        var hostRepository2 = new HostRepository(temporaryDirectory, EPHEMERAL_HOST_MINIMUM);

        var host1 = hostRepository1.reserveNextHost("foo");
        var host2 = hostRepository1.reserveNextHost("foo");
        var host3 = hostRepository1.reserveNextHost("foo");
        var host4 = hostRepository2.reserveNextHost("foo");
        var host5 = hostRepository2.reserveNextHost("foo");
        var host6 = hostRepository1.reserveNextHost("foo");

        assertThat(Set.of(host1, host2, host3, host4, host5, host6)).hasSize(6);
    }

    @Test
    void shouldNotOverrun() throws Exception {
        var hostRepository = new HostRepository(temporaryDirectory(), EPHEMERAL_HOST_MAXIMUM - 1);

        hostRepository.reserveNextHost("foo");
        hostRepository.reserveNextHost("foo");

        var exception = assertThrows(IllegalStateException.class, () -> hostRepository.reserveNextHost("foo"));
        assertThat(exception).hasMessage("There are no more hosts available");
    }

    private static Path temporaryDirectory() throws IOException {
        return Files.createTempDirectory("hostRepo");
    }
}
