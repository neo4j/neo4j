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
package org.neo4j.causalclustering.protocol.handshake;

import co.unruly.matchers.OptionalMatchers;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class ApplicationProtocolRepositoryTest
{
    private ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository(
            TestApplicationProtocols.values(), new ApplicationSupportedProtocols( RAFT, TestApplicationProtocols.listVersionsOf( RAFT ) ) );

    @Test
    public void shouldReturnEmptyIfUnknownVersion()
    {
        // when
        Optional<Protocol.ApplicationProtocol> applicationProtocol =
                applicationProtocolRepository.select( RAFT.canonicalName(), -1 );

        // then
        assertThat( applicationProtocol, OptionalMatchers.empty() );
    }

    @Test
    public void shouldReturnEmptyIfUnknownName()
    {
        // when
        Optional<Protocol.ApplicationProtocol> applicationProtocol = applicationProtocolRepository.select( "not a real protocol", 1 );

        // then
        assertThat( applicationProtocol, OptionalMatchers.empty() );
    }

    @Test
    public void shouldReturnEmptyIfNoVersions()
    {
        // when
        Optional<Protocol.ApplicationProtocol> applicationProtocol =
                applicationProtocolRepository.select( RAFT.canonicalName(), emptySet() );

        // then
        assertThat( applicationProtocol, OptionalMatchers.empty() );
    }

    @Test
    public void shouldReturnProtocolIfKnownNameAndVersion()
    {
        // when
        Optional<Protocol.ApplicationProtocol> applicationProtocol =
                applicationProtocolRepository.select( RAFT.canonicalName(), 1 );

        // then
        assertThat( applicationProtocol, OptionalMatchers.contains( TestApplicationProtocols.RAFT_1 ) );
    }

    @Test
    public void shouldReturnKnownProtocolVersionWhenFirstGivenVersionNotKnown()
    {
        // when
        Optional<Protocol.ApplicationProtocol> applicationProtocol =
                applicationProtocolRepository.select( RAFT.canonicalName(), asSet( -1, 1 ) );

        // then
        assertThat( applicationProtocol, OptionalMatchers.contains( TestApplicationProtocols.RAFT_1 ) );
    }

    @Test
    public void shouldReturnApplicationProtocolOfHighestVersionNumberRequestedAndSupported()
    {
        // when
        Optional<Protocol.ApplicationProtocol> applicationProtocol =
                applicationProtocolRepository.select( RAFT.canonicalName(), asSet( 389432, 1, 3, 2, 71234 ) );

        // then
        assertThat( applicationProtocol, OptionalMatchers.contains( TestApplicationProtocols.RAFT_3 ) );
    }

    @Test
    public void shouldIncludeAllProtocolsInSelectionIfEmptyVersionsProvided()
    {
        // when
        ProtocolSelection<Integer,Protocol.ApplicationProtocol> protocolSelection =
                applicationProtocolRepository.getAll( RAFT, emptyList() );

        // then
        Integer[] expectedRaftVersions = TestApplicationProtocols.allVersionsOf( RAFT );
        assertThat( protocolSelection.versions(), Matchers.containsInAnyOrder( expectedRaftVersions ) );
    }

    @Test
    public void shouldIncludeProtocolsInSelectionWithVersionsLimitedByThoseConfigured()
    {
        // given
        Integer[] expectedRaftVersions = { 1 };

        // when
        ProtocolSelection<Integer,Protocol.ApplicationProtocol> protocolSelection =
                applicationProtocolRepository.getAll( RAFT, asList( expectedRaftVersions ) );

        // then
        assertThat( protocolSelection.versions(), Matchers.containsInAnyOrder( expectedRaftVersions ) );
    }

    @Test
    public void shouldIncludeProtocolsInSelectionWithVersionsLimitedByThoseExisting()
    {
        // given
        Integer[] expectedRaftVersions = TestApplicationProtocols.allVersionsOf( RAFT );
        List<Integer> configuredRaftVersions =
                Stream.concat( Stream.of( expectedRaftVersions ), Stream.of( Integer.MAX_VALUE ) ).collect( Collectors.toList() );

        // when
        ProtocolSelection<Integer,Protocol.ApplicationProtocol> protocolSelection =
                applicationProtocolRepository.getAll( RAFT, configuredRaftVersions );

        // then
        assertThat( protocolSelection.versions(), Matchers.containsInAnyOrder( expectedRaftVersions ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowIfNoIntersectionBetweenExistingAndConfiguredVersions()
    {
        // given
        List<Integer> configuredRaftVersions = Arrays.asList( Integer.MAX_VALUE );

        // when
        applicationProtocolRepository.getAll( RAFT, configuredRaftVersions );

        // then throw
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotInstantiateIfDuplicateProtocolsSupplied()
    {
        // given
        Protocol.ApplicationProtocol protocol = new Protocol.ApplicationProtocol()
        {
            @Override
            public String category()
            {
                return "foo";
            }

            @Override
            public Integer implementation()
            {
                return 1;
            }
        };
        Protocol.ApplicationProtocol[] protocols = {protocol, protocol};

        // when
        new ApplicationProtocolRepository( protocols, new ApplicationSupportedProtocols( RAFT, TestApplicationProtocols.listVersionsOf( RAFT ) ) );

        // then throw
    }
}
