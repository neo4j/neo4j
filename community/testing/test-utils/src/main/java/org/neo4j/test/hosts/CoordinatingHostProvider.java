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

/**
 * Host provider that relies on state on disk, so that it can coordinate with other {@link CoordinatingHostProvider}s in
 * other JVMs. Suitable for parallel test execution.
 */
public class CoordinatingHostProvider implements HostProvider {
    private final HostRepository hostRepository;

    CoordinatingHostProvider(HostRepository hostRepository) {
        this.hostRepository = hostRepository;
    }

    @Override
    public long getNextFreeHost(String trace) {
        return hostRepository.reserveNextHost(trace);
    }
}
