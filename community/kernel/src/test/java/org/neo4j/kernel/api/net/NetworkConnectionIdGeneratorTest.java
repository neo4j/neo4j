/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.net;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NetworkConnectionIdGeneratorTest
{
    @Test
    void shouldGenerateIds()
    {
        NetworkConnectionIdGenerator idGenerator = new NetworkConnectionIdGenerator();

        assertEquals( "bolt-0", idGenerator.newConnectionId( "bolt" ) );
        assertEquals( "bolt-1", idGenerator.newConnectionId( "bolt" ) );
        assertEquals( "bolt-2", idGenerator.newConnectionId( "bolt" ) );

        assertEquals( "http-3", idGenerator.newConnectionId( "http" ) );
        assertEquals( "http-4", idGenerator.newConnectionId( "http" ) );

        assertEquals( "https-5", idGenerator.newConnectionId( "https" ) );
        assertEquals( "https-6", idGenerator.newConnectionId( "https" ) );
        assertEquals( "https-7", idGenerator.newConnectionId( "https" ) );
    }
}
