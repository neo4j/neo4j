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
package org.neo4j.causalclustering.core;

import co.unruly.matchers.StreamMatchers;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.SupportedProtocols;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols.COMPRESSION_SNAPPY;

public class SupportedProtocolCreatorTest
{

    private NullLogProvider log = NullLogProvider.getInstance();

    @Test
    public void shouldReturnRaftProtocol()
    {
        // given
        Config config = Config.defaults();

        // when
        ApplicationSupportedProtocols supportedRaftProtocol = new SupportedProtocolCreator( config, log ).createSupportedRaftProtocol();

        // then
        assertThat( supportedRaftProtocol.identifier(), equalTo( Protocol.ApplicationProtocolCategory.RAFT ) );
    }

    @Test
    public void shouldReturnEmptyVersionSupportedRaftProtocolIfNoVersionsConfigured()
    {
        // given
        Config config = Config.defaults();

        // when
        ApplicationSupportedProtocols supportedRaftProtocol = new SupportedProtocolCreator( config, log ).createSupportedRaftProtocol();

        // then
        assertThat( supportedRaftProtocol.versions(), empty() );
    }

    @Test
    public void shouldFilterUnknownRaftImplementations()
    {
        // given
        Config config = Config.defaults( CausalClusteringSettings.raft_implementations, "1, 2, 3" );

        // when
        ApplicationSupportedProtocols supportedRaftProtocol = new SupportedProtocolCreator( config, log ).createSupportedRaftProtocol();

        // then
        assertThat( supportedRaftProtocol.versions(), contains( 1 ) );

    }

    @Test
    public void shouldReturnConfiguredRaftProtocolVersions()
    {
        // given
        Config config = Config.defaults( CausalClusteringSettings.raft_implementations, "1" );

        // when
        ApplicationSupportedProtocols supportedRaftProtocol = new SupportedProtocolCreator( config, log ).createSupportedRaftProtocol();

        // then
        assertThat( supportedRaftProtocol.versions(), contains( 1 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowIfVersionsSpecifiedButAllUnknown()
    {
        // given
        Config config = Config.defaults( CausalClusteringSettings.raft_implementations, String.valueOf( Integer.MAX_VALUE ) );

        // when
        ApplicationSupportedProtocols supportedRaftProtocol = new SupportedProtocolCreator( config, log ).createSupportedRaftProtocol();

        // then throw
    }

    @Test
    public void shouldNotReturnModifiersIfNoVersionsSpecified()
    {
        // given
        Config config = Config.defaults();

        // when
        List<ModifierSupportedProtocols> supportedModifierProtocols =
                new SupportedProtocolCreator( config, log ).createSupportedModifierProtocols();

        // then
        assertThat( supportedModifierProtocols, empty() );
    }

    @Test
    public void shouldReturnACompressionModifierIfCompressionVersionsSpecified()
    {
        // given
        Config config = Config.defaults( CausalClusteringSettings.compression_implementations, COMPRESSION_SNAPPY.implementation() );

        // when
        List<ModifierSupportedProtocols> supportedModifierProtocols =
                new SupportedProtocolCreator( config, log ).createSupportedModifierProtocols();

        // then
        Stream<Protocol.Category<Protocol.ModifierProtocol>> identifiers = supportedModifierProtocols.stream().map( SupportedProtocols::identifier );
        assertThat( identifiers, StreamMatchers.contains( Protocol.ModifierProtocolCategory.COMPRESSION ) );
    }

    @Test
    public void shouldReturnCompressionWithVersionsSpecified()
    {
        // given
        Config config = Config.defaults( CausalClusteringSettings.compression_implementations, COMPRESSION_SNAPPY.implementation() );

        // when
        List<ModifierSupportedProtocols> supportedModifierProtocols =
                new SupportedProtocolCreator( config, log ).createSupportedModifierProtocols();

        // then
        List<String> versions = supportedModifierProtocols.get( 0 ).versions();
        assertThat( versions, contains( COMPRESSION_SNAPPY.implementation() ) );
    }

    @Test
    public void shouldReturnCompressionWithVersionsSpecifiedCaseInsensitive()
    {
        // given
        Config config = Config.defaults( CausalClusteringSettings.compression_implementations, COMPRESSION_SNAPPY.implementation().toLowerCase() );

        // when
        List<ModifierSupportedProtocols> supportedModifierProtocols =
                new SupportedProtocolCreator( config, log ).createSupportedModifierProtocols();

        // then
        List<String> versions = supportedModifierProtocols.get( 0 ).versions();
        assertThat( versions, contains( COMPRESSION_SNAPPY.implementation() ) );
    }
}
