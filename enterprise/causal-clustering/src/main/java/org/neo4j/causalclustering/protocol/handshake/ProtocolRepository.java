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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.stream.Streams;

public abstract class ProtocolRepository<T extends Protocol>
{
    private final Map<Pair<String, Integer>,T> protocolMap;
    private Function<String,Comparator<T>> comparator;

    public ProtocolRepository( T[] protocols, Function<String,Comparator<T>> comparators )
    {
        Map<Pair<String, Integer>,T> map = new HashMap<>();
        for ( T protocol : protocols )
        {
            Protocol previous = map.put( Pair.of( protocol.identifier(), protocol.version() ), protocol );
            if ( previous != null )
            {
                throw new IllegalArgumentException(
                        String.format( "Multiple protocols with same identifier and version supplied: %s and %s", protocol, previous ) );
            }
        }
        protocolMap = Collections.unmodifiableMap( map );
        this.comparator = comparators;
    }

    Optional<T> select( String protocolName, Integer version )
    {
        return Optional.ofNullable( protocolMap.get( Pair.of( protocolName, version ) ) );
    }

    Optional<T> select( String protocolName, Set<Integer> versions )
    {
        return versions
                .stream()
                .map( version -> select( protocolName, version ) )
                .flatMap( Streams::ofOptional )
                .max( comparator.apply( protocolName ) );
    }

    public ProtocolSelection<T> getAll( Protocol.Identifier<T> identifier, Collection<Integer> versions )
    {
        Set<Integer> selectedVersions = protocolMap
                .entrySet()
                .stream()
                .map( Map.Entry::getKey )
                .filter( pair -> pair.first().equals( identifier.canonicalName() ) )
                .map( Pair::other )
                .filter( version -> versions.isEmpty() || versions.contains( version ) )
                .collect( Collectors.toSet() );

        if ( selectedVersions.isEmpty() )
        {
            throw new IllegalArgumentException( String.format(
                    "Attempted to select protocols for %s versions %s but no match in known protocols %s",
                    identifier, versions, protocolMap
            ) );
        }
        else
        {
            return new ProtocolSelection<>( identifier.canonicalName(), selectedVersions );
        }
    }

    static <T extends Protocol> Comparator<T> versionNumberComparator()
    {
        return Comparator.comparingInt( Protocol::version );
    }
}
