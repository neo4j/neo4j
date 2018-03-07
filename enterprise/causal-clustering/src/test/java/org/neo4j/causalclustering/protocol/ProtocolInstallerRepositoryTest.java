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
package org.neo4j.causalclustering.protocol;

import org.hamcrest.Matchers;
import org.junit.Test;

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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ProtocolInstallerRepositoryTest
{
    private List<ModifierProtocolInstaller<Orientation.Client>> clientModifiers =
            asList( new SnappyClientInstaller(), new LZHClientInstaller(), new Rot13ClientInstaller() );
    private List<ModifierProtocolInstaller<Orientation.Server>> serverModifiers =
            asList( new SnappyServerInstaller(), new LZHServerInstaller(), new Rot13ServerInstaller() );

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
    public void shouldReturnModifierProtocolsForServer()
    {
        // given
        Protocol.ModifierProtocol expected = TestModifierProtocols.SNAPPY;
        ProtocolStack protocolStack = new ProtocolStack( ApplicationProtocols.RAFT_1, asList( expected ) );

        // when
        List<Protocol.ModifierProtocol> actual = clientRepository.installerFor( protocolStack ).modifiers();

        // then
        assertThat(
                expected,
                Matchers.isIn( actual )
        );
    }

    @Test
    public void shouldUseDifferentInstancesOfProtocolInstaller()
    {
        // given
        ProtocolStack protocolStack1 = new ProtocolStack( ApplicationProtocols.RAFT_1, asList( TestModifierProtocols.SNAPPY ) );
        ProtocolStack protocolStack2 = new ProtocolStack( ApplicationProtocols.RAFT_1, asList( TestModifierProtocols.LZH ) );

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
                asList( TestModifierProtocols.SNAPPY, TestModifierProtocols.LZH ) );

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

    private static class SnappyClientInstaller implements ModifierProtocolInstaller<Orientation.Client>
    {
        @Override
        public Protocol.ModifierProtocol protocol()
        {
            return TestModifierProtocols.SNAPPY;
        }

        @Override
        public <BUILDER extends NettyPipelineBuilder<Orientation.Client,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Client,BUILDER> nettyPipelineBuilder )
        {

        }
    }

    private static class LZHClientInstaller implements ModifierProtocolInstaller<Orientation.Client>
    {
        @Override
        public Protocol.ModifierProtocol protocol()
        {
            return TestModifierProtocols.LZH;
        }

        @Override
        public <BUILDER extends NettyPipelineBuilder<Orientation.Client,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Client,BUILDER> nettyPipelineBuilder )
        {

        }
    }

    private class Rot13ClientInstaller implements ModifierProtocolInstaller<Orientation.Client>
    {
        @Override
        public Protocol.ModifierProtocol protocol()
        {
            return TestModifierProtocols.ROT13;
        }

        @Override
        public <BUILDER extends NettyPipelineBuilder<Orientation.Client,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Client,BUILDER> nettyPipelineBuilder )
        {

        }
    }

    private static class SnappyServerInstaller implements ModifierProtocolInstaller<Orientation.Server>
    {
        @Override
        public Protocol.ModifierProtocol protocol()
        {
            return TestModifierProtocols.SNAPPY;
        }

        @Override
        public <BUILDER extends NettyPipelineBuilder<Orientation.Server,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Server,BUILDER> nettyPipelineBuilder )
        {

        }
    }

    private static class LZHServerInstaller implements ModifierProtocolInstaller<Orientation.Server>
    {
        @Override
        public Protocol.ModifierProtocol protocol()
        {
            return TestModifierProtocols.LZH;
        }

        @Override
        public <BUILDER extends NettyPipelineBuilder<Orientation.Server,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Server,BUILDER> nettyPipelineBuilder )
        {

        }
    }

    private class Rot13ServerInstaller implements ModifierProtocolInstaller<Orientation.Server>
    {
        @Override
        public Protocol.ModifierProtocol protocol()
        {
            return TestModifierProtocols.ROT13;
        }

        @Override
        public <BUILDER extends NettyPipelineBuilder<Orientation.Server,BUILDER>> void apply(
                NettyPipelineBuilder<Orientation.Server,BUILDER> nettyPipelineBuilder )
        {

        }
    }
}
