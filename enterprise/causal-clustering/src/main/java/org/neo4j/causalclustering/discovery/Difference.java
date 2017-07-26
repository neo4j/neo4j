/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import org.neo4j.causalclustering.identity.MemberId;

public class Difference<T>
{
    private MemberId memberId;
    private T server;

    private Difference( MemberId memberId, T server )
    {
        this.memberId = memberId;
        this.server = server;
    }

    static Difference<CoreServerInfo> asDifference( CoreTopology topology, MemberId memberId )
    {
        return new Difference<>( memberId, topology.find( memberId ).orElse( null ) );
    }

    static Difference<ReadReplicaInfo> asDifference( ReadReplicaTopology topology, MemberId memberId )
    {
        return new Difference<>( memberId, topology.find( memberId ).orElse( null ) );
    }

    public T getServer()
    {
        return server;
    }

    @Override
    public String toString()
    {
        return String.format( "{memberId=%s, info=%s}", memberId, server );
    }
}
