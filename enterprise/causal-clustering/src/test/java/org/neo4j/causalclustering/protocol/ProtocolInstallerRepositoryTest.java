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

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.RaftProtocolClientInstaller;
import org.neo4j.causalclustering.core.consensus.RaftProtocolServerInstaller;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ProtocolInstallerRepositoryTest
{
    private final NettyPipelineBuilderFactory pipelineBuilderFactory = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );
    private final RaftProtocolClientInstaller raftProtocolClientInstaller =
            new RaftProtocolClientInstaller( NullLogProvider.getInstance(), pipelineBuilderFactory );
    private final RaftProtocolServerInstaller raftProtocolServerInstaller =
            new RaftProtocolServerInstaller( null, pipelineBuilderFactory, NullLogProvider.getInstance() );

    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> clientRepository =
            new ProtocolInstallerRepository<>( asList( raftProtocolClientInstaller ) );
    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> serverRepository =
            new ProtocolInstallerRepository<>( asList( raftProtocolServerInstaller ) );

    @Test
    public void shouldReturnRaftServerInstaller() throws Throwable
    {
        assertEquals( raftProtocolServerInstaller, serverRepository.installerFor( Protocol.Protocols.RAFT_1 ) );
    }

    @Test
    public void shouldReturnRaftClientInstaller() throws Throwable
    {
        assertEquals( raftProtocolClientInstaller, clientRepository.installerFor( Protocol.Protocols.RAFT_1 ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotInitialiseIfMultipleInstallersForSameProtocolForServer() throws Throwable
    {
        new ProtocolInstallerRepository<>( asList( raftProtocolServerInstaller, raftProtocolServerInstaller ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotInitialiseIfMultipleInstallersForSameProtocolForClient() throws Throwable
    {
        new ProtocolInstallerRepository<>( asList( raftProtocolClientInstaller, raftProtocolClientInstaller ) );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIfUnknownProtocolForServer() throws Throwable
    {
        serverRepository.installerFor( TestProtocols.RAFT_3 );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIfUnknownProtocolForClient() throws Throwable
    {
        clientRepository.installerFor( TestProtocols.RAFT_3 );
    }
}
