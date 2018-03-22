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
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.Protocol;

public interface TestProtocols
{
    static <U extends Comparable<U>,T extends Protocol<U>> T latest( Protocol.Category<T> category, T[] values )
    {
        return Stream.of( values )
                .filter( protocol -> protocol.category().equals( category.canonicalName() ) )
                .max( Comparator.comparing( T::implementation ) )
                .get();
    }

    static <U extends Comparable<U>,T extends Protocol> U[] allVersionsOf( Protocol.Category<T> category, T[] values, IntFunction<U[]> constructor )
    {
        return Stream.of( values )
                .filter( protocol -> protocol.category().equals( category.canonicalName() ) )
                .map( Protocol::implementation )
                .toArray( constructor );
    }

    enum TestApplicationProtocols implements Protocol.ApplicationProtocol
    {
        RAFT_1( ApplicationProtocolCategory.RAFT, 1 ),
        RAFT_2( ApplicationProtocolCategory.RAFT, 2 ),
        RAFT_3( ApplicationProtocolCategory.RAFT, 3 ),
        RAFT_4( ApplicationProtocolCategory.RAFT, 4 ),
        CATCHUP_1( ApplicationProtocolCategory.CATCHUP, 1 ),
        CATCHUP_2( ApplicationProtocolCategory.CATCHUP, 2 ),
        CATCHUP_3( ApplicationProtocolCategory.CATCHUP, 3 ),
        CATCHUP_4( ApplicationProtocolCategory.CATCHUP, 4 );

        private final Integer version;

        private final ApplicationProtocolCategory identifier;
        TestApplicationProtocols( ApplicationProtocolCategory identifier, int version )
        {
            this.identifier = identifier;
            this.version = version;
        }

        @Override
        public String category()
        {
            return this.identifier.canonicalName();
        }

        @Override
        public Integer implementation()
        {
            return version;
        }

        public static ApplicationProtocol latest( ApplicationProtocolCategory identifier )
        {
            return TestProtocols.latest( identifier, values() );
        }

        public static Integer[] allVersionsOf( ApplicationProtocolCategory identifier )
        {
            return TestProtocols.allVersionsOf( identifier, TestApplicationProtocols.values(), Integer[]::new );
        }

        public static List<Integer> listVersionsOf( ApplicationProtocolCategory identifier )
        {
            return Arrays.asList( allVersionsOf( identifier ) );
        }
    }

    enum TestModifierProtocols implements Protocol.ModifierProtocol
    {
        SNAPPY( ModifierProtocolCategory.COMPRESSION, "TestSnappy" ),
        LZO( ModifierProtocolCategory.COMPRESSION, "TestLZO" ),
        LZ4( ModifierProtocolCategory.COMPRESSION, "TestLZ4" ),
        LZ4_VALIDATING( ModifierProtocolCategory.COMPRESSION, "TestLZ4Validating" ),
        LZ4_HIGH_COMPRESSION( ModifierProtocolCategory.COMPRESSION, "TestLZ4High" ),
        LZ4_HIGH_COMPRESSION_VALIDATING( ModifierProtocolCategory.COMPRESSION, "TestLZ4HighValidating" ),
        ROT13( ModifierProtocolCategory.GRATUITOUS_OBFUSCATION, "ROT13" ),
        NAME_CLASH( ModifierProtocolCategory.GRATUITOUS_OBFUSCATION, "TestSnappy" );

        private final ModifierProtocolCategory identifier;
        private final String friendlyName;

        TestModifierProtocols( ModifierProtocolCategory identifier, String friendlyName )
        {
            this.identifier = identifier;
            this.friendlyName = friendlyName;
        }

        @Override
        public String category()
        {
            return identifier.canonicalName();
        }

        @Override
        public String implementation()
        {
            return friendlyName;
        }

        public static ModifierProtocol latest( ModifierProtocolCategory identifier )
        {
            return TestProtocols.latest( identifier, values() );
        }

        public static String[] allVersionsOf( ModifierProtocolCategory identifier )
        {
            return TestProtocols.allVersionsOf( identifier, TestModifierProtocols.values(), String[]::new );
        }

        public static List<String> listVersionsOf( ModifierProtocolCategory identifier )
        {
            List<String> versions = Arrays.asList( allVersionsOf( identifier ) );
            versions.sort( Comparator.reverseOrder() );
            return versions;
        }
    }
}
