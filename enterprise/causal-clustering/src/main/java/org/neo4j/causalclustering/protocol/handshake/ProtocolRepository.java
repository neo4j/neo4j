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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.stream.Streams;

public class ProtocolRepository
{
    private final Map<Pair<String, Integer>,Protocol> protocolMap;

    public ProtocolRepository( Protocol[] protocols )
    {
        Map<Pair<String, Integer>,Protocol> map = new HashMap<>();
        for ( Protocol protocol : protocols )
        {
            Protocol previous = map.put( Pair.of( protocol.identifier(), protocol.version() ), protocol );
            if ( previous != null )
            {
                throw new IllegalArgumentException(
                        String.format( "Multiple protocols with same identifier and version supplied: %s and %s", protocol, previous ) );
            }
        }
        protocolMap = Collections.unmodifiableMap( map );
    }

    Optional<Protocol> select( String protocolName, int version )
    {
        return Optional.ofNullable( protocolMap.get( Pair.of( protocolName, version ) ) );
    }

    Optional<Protocol> select( String protocolName, Set<Integer> versions )
    {
        return versions
                .stream()
                .map( version -> of( protocolName, version ) )
                .flatMap( Streams::ofOptional )
                .max( Comparator.comparingInt( Protocol::version ) );
    }

    private Optional<Protocol> of( String identifier, Integer version )
    {
        return Optional.ofNullable( protocolMap.get( Pair.of( identifier, version) ) );
    }

    public ProtocolSelection getAll( Protocol.Identifier identifier )
    {
        Set<Integer> versions = protocolMap.entrySet().stream().filter( e -> e.getKey().first().equals( identifier.canonicalName() ) ).map(
                e -> e.getKey().other() ).collect( Collectors.toSet() );
        return new ProtocolSelection( identifier.canonicalName(), versions );
    }
}
