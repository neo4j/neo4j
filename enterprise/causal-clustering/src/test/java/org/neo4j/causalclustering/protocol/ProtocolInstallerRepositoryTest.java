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
package org.neo4j.causalclustering.protocol;

import org.junit.Test;

import java.util.Collection;
import java.util.List;

import org.neo4j.causalclustering.core.consensus.RaftProtocolClientInstaller;
import org.neo4j.causalclustering.core.consensus.RaftProtocolServerInstaller;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ProtocolInstallerRepositoryTest
{
    private List<ModifierProtocolInstaller<Orientation.Client>> clientModifiers =
            asList( new SnappyClientInstaller(),
                    new LZOClientInstaller(),
                    new LZ4ClientInstaller(),
                    new LZ4HighCompressionClientInstaller(),
                    new Rot13ClientInstaller() );
    private List<ModifierProtocolInstaller<Orientation.Server>> serverModifiers =
            asList( new SnappyServerInstaller(),
                    new LZOServerInstaller(),
                    new LZ4ServerInstaller(),
                    new LZ4ValidatingServerInstaller(),
                    new Rot13ServerInstaller() );

    private final NettyPipelineBuilderFactory pipelineBuilderFactory =
            new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );
    private final RaftProtocolClientInstaller.Factory raftProtocolClientInstaller =
            new RaftProtocolClientInstaller.Factory( pipelineBuilderFactory, NullLogProvider.getInstance() );
    private final RaftProtocolServerInstaller.Factory raftProtocolServerInstaller =
            new RaftProtocolServerInstaller.Factory( null, pipelineBuilderFactory, NullLogProvider.getInstance() );

    private final ProtocolInstallerRepository<Orientation.Client> clientRepository =
            new ProtocolInstallerRepository<>( asList( raftProtocolClientInstaller ), clientModifiers );
    private final ProtocolInstallerRepository<Orientation.Server> serverRepository =
            new ProtocolInstallerRepository<>( asList( raftProtocolServerInstaller ), serverModifiers );

    @Test
    public void shouldReturnRaftServerInstaller()
    {
        assertEquals(
                raftProtocolServerInstaller.applicationProtocol(),
                serverRepository.installerFor( new ProtocolStack( ApplicationProtocols.RAFT_1, emptyList() ) ).applicationProtocol() );
    }

    @Test
    public void shouldReturnRaftClientInstaller()
    {
        assertEquals(
                raftProtocolClientInstaller.applicationProtocol(),
                clientRepository.installerFor( new ProtocolStack( ApplicationProtocols.RAFT_1, emptyList() ) ).applicationProtocol() );
    }

    @Test
    public void shouldReturnModifierProtocolsForClient()
    {
        // given
        Protocol.ModifierProtocol expected = TestModifierProtocols.SNAPPY;
        ProtocolStack protocolStack = new ProtocolStack( ApplicationProtocols.RAFT_1, asList( expected ) );

        // when
        Collection<Collection<Protocol.ModifierProtocol>> actual = clientRepository.installerFor( protocolStack ).modifiers();

        // then
        assertThat( actual, contains( contains( expected ) ) );
    }

    @Test
    public void shouldReturnModifierProtocolsForServer()
    {
        // given
        Protocol.ModifierProtocol expected = TestModifierProtocols.SNAPPY;
        ProtocolStack protocolStack = new ProtocolStack( ApplicationProtocols.RAFT_1, asList( expected ) );

        // when
        Collection<Collection<Protocol.ModifierProtocol>> actual = serverRepository.installerFor( protocolStack ).modifiers();

        // then
        assertThat( actual, contains( contains( expected ) ) );
    }

    @Test
    public void shouldReturnModifierProtocolsForProtocolWithSharedInstallerForClient()
    {
        // given
        Protocol.ModifierProtocol expected = TestModifierProtocols.LZ4_HIGH_COMPRESSION_VALIDATING;
        TestModifierProtocols alsoSupported = TestModifierProtocols.LZ4_HIGH_COMPRESSION;

        ProtocolStack protocolStack = new ProtocolStack( ApplicationProtocols.RAFT_1, asList( expected ) );

        // when
        Collection<Collection<Protocol.ModifierProtocol>> actual = clientRepository.installerFor( protocolStack ).modifiers();

        // then
        assertThat( actual, contains( containsInAnyOrder( expected, alsoSupported ) )) ;
    }

    @Test
    public void shouldReturnModifierProtocolsForProtocolWithSharedInstallerForServer()
    {
        // given
        Protocol.ModifierProtocol expected = TestModifierProtocols.LZ4_HIGH_COMPRESSION_VALIDATING;
        TestModifierProtocols alsoSupported = TestModifierProtocols.LZ4_VALIDATING;

        ProtocolStack protocolStack = new ProtocolStack( ApplicationProtocols.RAFT_1, asList( expected ) );

        // when
        Collection<Collection<Protocol.ModifierProtocol>> actual = serverRepository.installerFor( protocolStack ).modifiers();

        // then
        assertThat( actual, contains( containsInAnyOrder( expected, alsoSupported ) )) ;
    }

    @Test
    public void shouldUseDifferentInstancesOfProtocolInstaller()
    {
        // given
        ProtocolStack protocolStack1 = new ProtocolStack( ApplicationProtocols.RAFT_1, asList( TestModifierProtocols.SNAPPY ) );
        ProtocolStack protocolStack2 = new ProtocolStack( ApplicationProtocols.RAFT_1, asList( TestModifierProtocols.LZO ) );

        // when
        ProtocolInstaller protocolInstaller1 = clientRepository.installerFor( protocolStack1 );
        ProtocolInstaller protocolInstaller2 = clientRepository.installerFor( protocolStack2 );

        // then
        assertThat( protocolInstaller1, not( sameInstance( protocolInstaller2 ) ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowIfAttemptingToCreateInstallerForMultipleModifiersWithSameIdentifier()
    {
        // given
        ProtocolStack protocolStack = new ProtocolStack(
                ApplicationProtocols.RAFT_1,
                asList( TestModifierProtocols.SNAPPY, TestModifierProtocols.LZO ) );

        // when
        clientRepository.installerFor( protocolStack );

        // then throw
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotInitialiseIfMultipleInstallersForSameProtocolForServer()
    {
        new ProtocolInstallerRepository<>( asList( raftProtocolServerInstaller, raftProtocolServerInstaller ), emptyList() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotInitialiseIfMultipleInstallersForSameProtocolForClient()
    {
        new ProtocolInstallerRepository<>( asList( raftProtocolClientInstaller, raftProtocolClientInstaller ), emptyList() );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIfUnknownProtocolForServer()
    {
        serverRepository.installerFor( new ProtocolStack( TestApplicationProtocols.RAFT_3, emptyList() ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIfUnknownProtocolForClient()
    {
        clientRepository.installerFor( new ProtocolStack( TestApplicationProtocols.RAFT_3, emptyList() ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIfUnknownModifierProtocol()
    {
        // given
        // setup used TestModifierProtocols, doesn't know about production protocols
        Protocol.ModifierProtocol unknownProtocol = ModifierProtocols.COMPRESSION_SNAPPY;

        // when
        serverRepository.installerFor( new ProtocolStack( ApplicationProtocols.RAFT_1, asList( unknownProtocol ) ) );

        // then throw
    }

    // Dummy installers

    private static class SnappyClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        private SnappyClientInstaller()
        {
            super( "snappy", null, TestModifierProtocols.SNAPPY );
        }
    }

    private static class LZOClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        private LZOClientInstaller()
        {
            super( "lzo", null, TestModifierProtocols.LZO );
        }
    }

    private static class LZ4ClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        private LZ4ClientInstaller()
        {
            super( "lz4", null, TestModifierProtocols.LZ4, TestModifierProtocols.LZ4_VALIDATING );
        }
    }
    private static class LZ4HighCompressionClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        private LZ4HighCompressionClientInstaller()
        {
            super( "lz4", null, TestModifierProtocols.LZ4_HIGH_COMPRESSION, TestModifierProtocols.LZ4_HIGH_COMPRESSION_VALIDATING );
        }
    }

    private class Rot13ClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        Rot13ClientInstaller()
        {
            super( "rot13", null, TestModifierProtocols.ROT13 );
        }
    }

    private static class SnappyServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        private SnappyServerInstaller()
        {
            super( "snappy", null, TestModifierProtocols.SNAPPY );
        }
    }

    private static class LZOServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        private LZOServerInstaller()
        {
            super( "lzo", null, TestModifierProtocols.LZO );
        }
    }

    private static class LZ4ServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        private LZ4ServerInstaller()
        {
            super( "lz4", null, TestModifierProtocols.LZ4, TestModifierProtocols.LZ4_HIGH_COMPRESSION );
        }
    }

    private static class LZ4ValidatingServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        private LZ4ValidatingServerInstaller()
        {
            super( "lz4", null, TestModifierProtocols.LZ4_VALIDATING, TestModifierProtocols.LZ4_HIGH_COMPRESSION_VALIDATING );
        }
    }

    private class Rot13ServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        Rot13ServerInstaller()
        {
            super( "rot13", null, TestModifierProtocols.ROT13 );
        }
    }
}
