/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.protocol.handshake;

import org.neo4j.causalclustering.protocol.Protocol;

public enum TestProtocols implements Protocol
{
    RAFT_1( Identifier.RAFT, 1 ),
    RAFT_2( Identifier.RAFT, 2 ),
    RAFT_3( Identifier.RAFT, 3 ),
    CATCHUP_1( Identifier.CATCHUP, 1 ),
    CATCHUP_2( Identifier.CATCHUP, 2 ),
    CATCHUP_3( Identifier.CATCHUP, 3 ),
    CATCHUP_4( Identifier.CATCHUP, 4 );

    private final int version;
    private final Identifier identifier;

    TestProtocols( Identifier identifier, int version )
    {
        this.identifier = identifier;
        this.version = version;
    }

    @Override
    public String identifier()
    {
        return this.identifier.canonicalName();
    }

    @Override
    public int version()
    {
        return version;
    }
}
