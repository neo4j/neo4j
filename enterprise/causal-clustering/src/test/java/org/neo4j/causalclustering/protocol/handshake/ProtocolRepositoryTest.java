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

import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.causalclustering.protocol.Protocol;

import static co.unruly.matchers.OptionalMatchers.contains;
import static co.unruly.matchers.OptionalMatchers.empty;
import static java.util.Collections.emptySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.causalclustering.protocol.Protocol.Identifier.RAFT;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.RAFT_1;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.RAFT_3;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ProtocolRepositoryTest
{
    private ProtocolRepository protocolRepository = new ProtocolRepository( TestProtocols.values() );

    @Test
    public void shouldReturnEmptyIfUnknownVersion()
    {
        // when
        Optional<Protocol> applicationProtocol = protocolRepository.select( RAFT.canonicalName(), -1 );

        // then
        assertThat( applicationProtocol, empty() );
    }

    @Test
    public void shouldReturnEmptyIfUnknownName()
    {
        // when
        Optional<Protocol> applicationProtocol = protocolRepository.select( "not a real protocol", 1 );

        // then
        assertThat( applicationProtocol, empty() );
    }

    @Test
    public void shouldReturnEmptyIfNoVersions()
    {
        // when
        Optional<Protocol> applicationProtocol = protocolRepository.select( RAFT.canonicalName(), emptySet());

        // then
        assertThat( applicationProtocol, empty() );
    }

    @Test
    public void shouldReturnProtocolIfKnownNameAndVersion()
    {
        // when
        Optional<Protocol> applicationProtocol = protocolRepository.select( RAFT.canonicalName(), 1 );

        // then
        assertThat( applicationProtocol, contains( RAFT_1 ) );
    }

    @Test
    public void shouldReturnKnownProtocolVersionWhenFirstGivenVersionNotKnown()
    {
        // when
        Optional<Protocol> applicationProtocol = protocolRepository.select( RAFT.canonicalName(), asSet( -1, 1 ));

        // then
        assertThat( applicationProtocol, contains( RAFT_1 ) );
    }

    @Test
    public void shouldReturnProtocolOfHighestVersionRequestedAndSupported()
    {
        // when
        Optional<Protocol> applicationProtocol = protocolRepository.select( RAFT.canonicalName(), asSet( 9, 1, 3, 2, 7 ) );

        // then
        assertThat( applicationProtocol, contains( RAFT_3 ) );
    }

    @Test
    public void shouldNotInstantiateIfDuplicateProtocolsSupplied()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            // given
            Protocol protocol = new Protocol()
            {

                @Override
                public String identifier()
                {
                    return "foo";
                }

                @Override
                public int version()
                {
                    return 1;
                }
            };
            Protocol[] protocols = {protocol, protocol};

            // when
            new ProtocolRepository( protocols );

            // then throw

        } );
    }
}
