/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.discovery;

import org.neo4j.causalclustering.identity.MemberId;

class Difference
{
    private MemberId memberId;
    private CatchupServerAddress server;

    private Difference( MemberId memberId, CatchupServerAddress server )
    {
        this.memberId = memberId;
        this.server = server;
    }

    static <T extends DiscoveryServerInfo> Difference asDifference( Topology<T> topology, MemberId memberId )
    {
        return new Difference( memberId, topology.find( memberId ).orElse( null ) );
    }

    @Override
    public String toString()
    {
        return String.format( "{memberId=%s, info=%s}", memberId, server );
    }
}
