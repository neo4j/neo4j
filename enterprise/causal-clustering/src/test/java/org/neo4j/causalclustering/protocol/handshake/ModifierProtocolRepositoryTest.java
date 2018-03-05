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

import co.unruly.matchers.OptionalMatchers;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.neo4j.causalclustering.protocol.Protocol;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolIdentifier.COMPRESSION;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION;
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
        List<SupportedProtocols<Protocol.ModifierProtocol>> supportedProtocols = asList(
                new SupportedProtocols<>( COMPRESSION, asList( 2, 1, 3 ) ),
                new SupportedProtocols<>( GRATUITOUS_OBFUSCATION, asList( 1, 3, 2 ) ) );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( TestProtocols.TestModifierProtocols.values(), supportedProtocols );
        // when
        Optional<Protocol.ModifierProtocol> modifierProtocol =
                modifierProtocolRepository.select( COMPRESSION.canonicalName(), asSet( 9, 1, 3, 2, 7 ) );

        // then
        assertThat( modifierProtocol.map( Protocol::version ), OptionalMatchers.contains( 2 ) );
    }

    @Test
    public void shouldReturnModifierProtocolOfSingleConfiguredVersionIfOthersRequested()
    {
        // given
        List<SupportedProtocols<Protocol.ModifierProtocol>> supportedProtocols = asList(
                new SupportedProtocols<>( COMPRESSION, asList( 2 ) ) );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( TestProtocols.TestModifierProtocols.values(), supportedProtocols );
        // when
        Optional<Protocol.ModifierProtocol> modifierProtocol =
                modifierProtocolRepository.select( COMPRESSION.canonicalName(), asSet( TestProtocols.TestModifierProtocols.allVersionsOf( COMPRESSION ) ) );

        // then
        assertThat( modifierProtocol.map( Protocol::version ), OptionalMatchers.contains( 2 ) );
    }

    @Test
    public void shouldCompareModifierProtocolsByListOrder() throws Throwable
    {
        List<SupportedProtocols<Protocol.ModifierProtocol>> supportedProtocols = asList(
                new SupportedProtocols<>( COMPRESSION, asList( 2, 1, 3 ) ) );

        Comparator<Protocol.ModifierProtocol> comparator =
                ModifierProtocolRepository.getModifierProtocolComparator( supportedProtocols )
                .apply( COMPRESSION.canonicalName() );

        assertThat( comparator.compare( TestProtocols.TestModifierProtocols.LZO, TestProtocols.TestModifierProtocols.SNAPPY ), Matchers.greaterThan( 0 )  );
        assertThat( comparator.compare( TestProtocols.TestModifierProtocols.SNAPPY, TestProtocols.TestModifierProtocols.LZ4 ), Matchers.greaterThan( 0 )  );
    }
}
