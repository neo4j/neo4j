/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.ports.allocation;

/**
 * Port provider that relies on state on disk, so that it can coordinate with other {@link CoordinatingPortProvider}s in
 * other JVMs. Suitable for parallel test execution.
 */
public class CoordinatingPortProvider implements PortProvider
{
    private final PortRepository portRepository;
    private final PortProbe portProbe;

    public CoordinatingPortProvider( PortRepository portRepository, PortProbe portProbe )
    {
        this.portRepository = portRepository;
        this.portProbe = portProbe;
    }

    @Override
    public int getNextFreePort( String trace )
    {
        int port = portRepository.reserveNextPort( trace );

        while ( portProbe.isOccupied( port ) )
        {
            port = portRepository.reserveNextPort( trace );
        }

        return port;
    }
}
