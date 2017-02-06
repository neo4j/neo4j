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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.Pair;

import static java.util.Collections.emptyMap;


public class ReadReplicaTopology
{
    static final ReadReplicaTopology EMPTY = new ReadReplicaTopology( emptyMap() );

    private final Map<MemberId,ReadReplicaAddresses> readReplicaMembers;

    public ReadReplicaTopology( Map<MemberId,ReadReplicaAddresses> readReplicaMembers )
    {
        this.readReplicaMembers = readReplicaMembers;
    }

    public Collection<ReadReplicaAddresses> members()
    {
        return readReplicaMembers.values();
    }

    public Optional<ReadReplicaAddresses> find( MemberId memberId )
    {
        return Optional.ofNullable( readReplicaMembers.get( memberId ) );
    }

    @Override
    public String toString()
    {
        return String.format( "{readReplicas=%s}", readReplicaMembers );
    }

    public Optional<MemberId> anyReadReplicaMemberId()
    {
        if ( readReplicaMembers.keySet().size() == 0 )
        {
            return Optional.empty();
        }
        else
        {
            return readReplicaMembers.keySet().stream().findAny();
        }

    }
}
