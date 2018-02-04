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

import org.junit.jupiter.api.Test;

import org.neo4j.causalclustering.core.consensus.RaftProtocolClientInstaller;
import org.neo4j.causalclustering.core.consensus.RaftProtocolServerInstaller;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory.VOID_WRAPPER;
import static org.neo4j.causalclustering.protocol.Protocol.Protocols;
import static org.neo4j.causalclustering.protocol.Protocol.Protocols.RAFT_1;
import static org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import static org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Client;
import static org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Server;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.RAFT_3;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class ProtocolInstallerRepositoryTest
{
    private final NettyPipelineBuilderFactory pipelineBuilderFactory = new NettyPipelineBuilderFactory( VOID_WRAPPER );
    private final RaftProtocolClientInstaller raftProtocolClientInstaller =
            new RaftProtocolClientInstaller( getInstance(), pipelineBuilderFactory );
    private final RaftProtocolServerInstaller raftProtocolServerInstaller =
            new RaftProtocolServerInstaller( null, pipelineBuilderFactory, getInstance() );

    private final ProtocolInstallerRepository<Client> clientRepository =
            new ProtocolInstallerRepository<>( asList( raftProtocolClientInstaller ) );
    private final ProtocolInstallerRepository<Server> serverRepository =
            new ProtocolInstallerRepository<>( asList( raftProtocolServerInstaller ) );

    @Test
    public void shouldReturnRaftServerInstaller()
    {
        assertEquals( raftProtocolServerInstaller, serverRepository.installerFor( RAFT_1 ) );
    }

    @Test
    public void shouldReturnRaftClientInstaller()
    {
        assertEquals( raftProtocolClientInstaller, clientRepository.installerFor( RAFT_1 ) );
    }

    @Test
    public void shouldNotInitialiseIfMultipleInstallersForSameProtocolForServer()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            new ProtocolInstallerRepository<>( asList( raftProtocolServerInstaller, raftProtocolServerInstaller ) );
        } );
    }

    @Test
    public void shouldNotInitialiseIfMultipleInstallersForSameProtocolForClient()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            new ProtocolInstallerRepository<>( asList( raftProtocolClientInstaller, raftProtocolClientInstaller ) );
        } );
    }

    @Test
    public void shouldThrowIfUnknownProtocolForServer()
    {
        assertThrows( IllegalStateException.class, () -> {
            serverRepository.installerFor( RAFT_3 );
        } );
    }

    @Test
    public void shouldThrowIfUnknownProtocolForClient()
    {
        assertThrows( IllegalStateException.class, () -> {
            clientRepository.installerFor( RAFT_3 );
        } );
    }
}
