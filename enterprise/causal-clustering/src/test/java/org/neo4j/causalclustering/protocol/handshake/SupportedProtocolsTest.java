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

import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import org.neo4j.helpers.collection.Iterators;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;

public class SupportedProtocolsTest
{
    @Test
    public void shouldMutuallySupportIntersectionOfParameterVersionsSuperset()
    {
        // given
        ApplicationSupportedProtocols supportedProtocols = new ApplicationSupportedProtocols( RAFT, Arrays.asList( 1, 2 ) );

        // when
        Set<Integer> mutuallySupported = supportedProtocols.mutuallySupportedVersionsFor( Iterators.asSet( 1, 2, 3 ) );

        // then
        assertThat( mutuallySupported, containsInAnyOrder( 1, 2 ) );
    }

    @Test
    public void shouldMutuallySupportIntersectionOfParameterVersionsSubset()
    {
        // given
        ApplicationSupportedProtocols supportedProtocols = new ApplicationSupportedProtocols( RAFT, Arrays.asList( 4, 5, 6 ) );

        // when
        Set<Integer> mutuallySupported = supportedProtocols.mutuallySupportedVersionsFor( Iterators.asSet( 4, 5 ) );

        // then
        assertThat( mutuallySupported, containsInAnyOrder( 4, 5 ) );
    }

    @Test
    public void shouldMutuallySupportParameterIfEmptyVersions()
    {
        // given
        ApplicationSupportedProtocols supportedProtocols = new ApplicationSupportedProtocols( RAFT, emptyList() );

        // when
        Set<Integer> mutuallySupported = supportedProtocols.mutuallySupportedVersionsFor( Iterators.asSet( 7, 8 ) );

        // then
        assertThat( mutuallySupported, containsInAnyOrder( 7, 8 ) );
    }

    @Test
    public void shouldMutuallySupportNothingIfParametersEmpty()
    {
        // given
        ApplicationSupportedProtocols supportedProtocols = new ApplicationSupportedProtocols( RAFT, Arrays.asList( 1, 2 ) );

        // when
        Set<Integer> mutuallySupported = supportedProtocols.mutuallySupportedVersionsFor( emptySet() );

        // then
        assertThat( mutuallySupported, empty() );
    }
}
