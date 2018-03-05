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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.Protocol;

public interface TestProtocols
{
    static <T extends Protocol> T latest( Protocol.Identifier<T> identifier, T[] values )
    {
        return Stream.of( values )
                .filter( protocol -> protocol.identifier().equals( identifier.canonicalName() ) )
                .max( Comparator.comparing( T::version ) )
                .get();
    }

    static <T extends Protocol> Integer[] allVersionsOf( Protocol.Identifier<T> identifier, T[] values )
    {
        return Stream.of( values )
                .filter( protocol -> protocol.identifier().equals( identifier.canonicalName() ) )
                .map( Protocol::version )
                .toArray( Integer[]::new );
    }

    enum TestApplicationProtocols implements Protocol.ApplicationProtocol
    {
        RAFT_1( ApplicationProtocolIdentifier.RAFT, 1 ),
        RAFT_2( ApplicationProtocolIdentifier.RAFT, 2 ),
        RAFT_3( ApplicationProtocolIdentifier.RAFT, 3 ),
        RAFT_4( ApplicationProtocolIdentifier.RAFT, 4 ),
        CATCHUP_1( ApplicationProtocolIdentifier.CATCHUP, 1 ),
        CATCHUP_2( ApplicationProtocolIdentifier.CATCHUP, 2 ),
        CATCHUP_3( ApplicationProtocolIdentifier.CATCHUP, 3 ),
        CATCHUP_4( ApplicationProtocolIdentifier.CATCHUP, 4 );

        private final int version;

        private final ApplicationProtocolIdentifier identifier;
        TestApplicationProtocols( ApplicationProtocolIdentifier identifier, int version )
        {
            this.identifier = identifier;
            this.version = version;
        }

        @Override
        public String identifier()
        {
            return this.identifier.canonicalName();
        }

        @Override
        public int version()
        {
            return version;
        }

        public static ApplicationProtocol latest( ApplicationProtocolIdentifier identifier )
        {
            return TestProtocols.latest( identifier, values() );
        }

        public static Integer[] allVersionsOf( ApplicationProtocolIdentifier identifier )
        {
            return TestProtocols.allVersionsOf( identifier, TestApplicationProtocols.values() );
        }

        public static List<Integer> listVersionsOf( ApplicationProtocolIdentifier identifier )
        {
            return Arrays.asList( allVersionsOf( identifier ) );
        }
    }

    enum TestModifierProtocols implements Protocol.ModifierProtocol
    {
        SNAPPY( ModifierProtocolIdentifier.COMPRESSION, 1, "TestSnappy" ),
        LZO( ModifierProtocolIdentifier.COMPRESSION, 2, "TestLZO" ),
        LZ4( ModifierProtocolIdentifier.COMPRESSION, 3, "TestLZ4" ),
        LZ4_VALIDATING( ModifierProtocolIdentifier.COMPRESSION, 4, "TestLZ4Validating" ),
        LZ4_HIGH_COMPRESSION( ModifierProtocolIdentifier.COMPRESSION, 5, "TestLZ4High" ),
        LZ4_HIGH_COMPRESSION_VALIDATING( ModifierProtocolIdentifier.COMPRESSION, 6, "TestLZ4HighValidating" ),
        ROT13( ModifierProtocolIdentifier.GRATUITOUS_OBFUSCATION, 1, "ROT13" );

        private final int version;
        private final ModifierProtocolIdentifier identifier;
        private final String friendlyName;

        TestModifierProtocols( ModifierProtocolIdentifier identifier, int version, String friendlyName )
        {
            this.version = version;
            this.identifier = identifier;
            this.friendlyName = friendlyName;
        }

        @Override
        public String identifier()
        {
            return identifier.canonicalName();
        }

        @Override
        public int version()
        {
            return version;
        }

        @Override
        public String friendlyName()
        {
            return friendlyName;
        }

        public static ModifierProtocol latest( ModifierProtocolIdentifier identifier )
        {
            return TestProtocols.latest( identifier, values() );
        }

        public static Integer[] allVersionsOf( ModifierProtocolIdentifier identifier )
        {
            return TestProtocols.allVersionsOf( identifier, TestModifierProtocols.values() );
        }

        public static List<Integer> listVersionsOf( ModifierProtocolIdentifier identifier )
        {
            List<Integer> versions = Arrays.asList( allVersionsOf( identifier ) );
            versions.sort( Comparator.reverseOrder() );
            return versions;
        }
    }
}
