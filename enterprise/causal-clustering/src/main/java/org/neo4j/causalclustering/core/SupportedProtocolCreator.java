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
package org.neo4j.causalclustering.core;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.handshake.SupportedProtocols;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.stream.Streams;

public class SupportedProtocolCreator
{
    private final Config config;

    public SupportedProtocolCreator( Config config )
    {
        this.config = config;
    }

    public SupportedProtocols<Protocol.ApplicationProtocol> createSupportedRaftProtocol()
    {
        List<Integer> raftVersions = config.get( CausalClusteringSettings.raft_versions );
        return new SupportedProtocols<>( Protocol.ApplicationProtocolIdentifier.RAFT, raftVersions );
    }

    public List<SupportedProtocols<Protocol.ModifierProtocol>> createSupportedModifierProtocols()
    {
        SupportedProtocols<Protocol.ModifierProtocol> supportedCompression = compressionProtocolVersions();

        return Stream.of( supportedCompression )
                .filter( supportedProtocols -> !supportedProtocols.versions().isEmpty() )
                .collect( Collectors.toList() );
    }

    private SupportedProtocols<Protocol.ModifierProtocol> compressionProtocolVersions()
    {
        return modifierProtocolVersions( CausalClusteringSettings.compression_versions, Protocol.ModifierProtocolIdentifier.COMPRESSION );
    }

    private SupportedProtocols<Protocol.ModifierProtocol> modifierProtocolVersions(
            Setting<List<String>> compressionVersions, Protocol.ModifierProtocolIdentifier identifier )
    {
        List<String> compressionAlgorithms = config.get( compressionVersions );
        List<Integer> versions = compressionAlgorithms.stream()
                .map( Protocol.ModifierProtocols::fromFriendlyName )
                .flatMap( Streams::ofOptional )
                .filter( protocol -> Objects.equals( protocol.identifier(), identifier.canonicalName() ) )
                .map( Protocol.ModifierProtocols::version )
                .collect( Collectors.toList() );

        return new SupportedProtocols<>( identifier, versions );
    }
}
