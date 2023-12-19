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

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.neo4j.causalclustering.protocol.Protocol;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.GRATUITOUS_OBFUSCATION;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZ4;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.LZO;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.NAME_CLASH;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols.SNAPPY;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * @see ApplicationProtocolRepositoryTest for tests on base class
 */
public class ModifierProtocolRepositoryTest
{
    @Test
    public void shouldReturnModifierProtocolOfFirstConfiguredVersionRequestedAndSupported()
    {
        // given
        List<ModifierSupportedProtocols> supportedProtocols = asList(
                new ModifierSupportedProtocols( COMPRESSION, asList( LZO.implementation(), SNAPPY.implementation(), LZ4.implementation() ) ),
                new ModifierSupportedProtocols( GRATUITOUS_OBFUSCATION, asList( NAME_CLASH.implementation() ) ) );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( TestProtocols.TestModifierProtocols.values(), supportedProtocols );
        // when
        Optional<Protocol.ModifierProtocol> modifierProtocol = modifierProtocolRepository.select(
                COMPRESSION.canonicalName(),
                asSet( "bzip2", SNAPPY.implementation(), LZ4.implementation(), LZO.implementation(), "fast_lz" )
        );

        // then
        assertThat( modifierProtocol.map( Protocol::implementation), OptionalMatchers.contains( LZO.implementation() ) );
    }

    @Test
    public void shouldReturnModifierProtocolOfSingleConfiguredVersionIfOthersRequested()
    {
        // given
        List<ModifierSupportedProtocols> supportedProtocols = asList(
                new ModifierSupportedProtocols( COMPRESSION, asList( LZO.implementation() ) ) );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( TestProtocols.TestModifierProtocols.values(), supportedProtocols );
        // when
        Optional<Protocol.ModifierProtocol> modifierProtocol =
                modifierProtocolRepository.select( COMPRESSION.canonicalName(), asSet( TestProtocols.TestModifierProtocols.allVersionsOf( COMPRESSION ) ) );

        // then
        assertThat( modifierProtocol.map( Protocol::implementation), OptionalMatchers.contains( LZO.implementation() ) );
    }

    @Test
    public void shouldCompareModifierProtocolsByListOrder()
    {
        List<ModifierSupportedProtocols> supportedProtocols = asList(
                new ModifierSupportedProtocols( COMPRESSION, asList( LZO.implementation(), SNAPPY.implementation(), LZ4.implementation() ) ) );

        Comparator<Protocol.ModifierProtocol> comparator =
                ModifierProtocolRepository.getModifierProtocolComparator( supportedProtocols )
                .apply( COMPRESSION.canonicalName() );

        assertThat( comparator.compare( LZO, TestProtocols.TestModifierProtocols.SNAPPY ), Matchers.greaterThan( 0 )  );
        assertThat( comparator.compare( TestProtocols.TestModifierProtocols.SNAPPY, TestProtocols.TestModifierProtocols.LZ4 ), Matchers.greaterThan( 0 )  );
    }
}
