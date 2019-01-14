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

import static org.neo4j.ports.allocation.PortConstants.EphemeralPortMaximum;

/**
 * Port provider that relies on state in a single JVM. Not suitable for parallel test execution (as in, several JVM
 * processes executing tests). _Is_ suitable for multi-threaded execution.
 */
public class SimplePortProvider implements PortProvider
{
    private final PortProbe portProbe;

    private int currentPort;

    public SimplePortProvider( PortProbe portProbe, int initialPort )
    {
        this.portProbe = portProbe;

        this.currentPort = initialPort;
    }

    @Override
    public synchronized int getNextFreePort( String ignored )
    {
        while ( currentPort <= EphemeralPortMaximum )
        {
            if ( !portProbe.isOccupied( currentPort ) )
            {
                return currentPort++;
            }

            currentPort++;
        }

        throw new IllegalStateException( "There are no more ports available" );
    }
}
