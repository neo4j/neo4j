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
package org.neo4j.causalclustering.protocol;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public interface Protocol<IMPL extends Comparable<IMPL>>
{
    String category();

    IMPL implementation();

    static <IMPL extends Comparable<IMPL>, T extends Protocol<IMPL>> Optional<T> find(
            T[] values, Category<T> category, IMPL implementation, Function<IMPL,IMPL> normalise )
    {
        return Stream.of( values )
                .filter( protocol -> Objects.equals( protocol.category(), category.canonicalName() ) )
                .filter( protocol -> Objects.equals( normalise.apply( protocol.implementation() ), normalise.apply( implementation ) ) )
                .findFirst();
    }

    interface Category<T extends Protocol>
    {
        String canonicalName();
    }

    interface ApplicationProtocol extends Protocol<Integer>
    {
    }

    enum ApplicationProtocolCategory implements Category<ApplicationProtocol>
    {
        RAFT,
        CATCHUP;

        @Override
        public String canonicalName()
        {
            return name().toLowerCase();
        }
    }

    enum ApplicationProtocols implements ApplicationProtocol
    {
        RAFT_1( ApplicationProtocolCategory.RAFT, 1 ),
        CATCHUP_1( ApplicationProtocolCategory.CATCHUP, 1 );

        private final Integer version;
        private final ApplicationProtocolCategory identifier;

        ApplicationProtocols( ApplicationProtocolCategory identifier, int version )
        {
            this.identifier = identifier;
            this.version = version;
        }

        @Override
        public String category()
        {
            return identifier.canonicalName();
        }

        @Override
        public Integer implementation()
        {
            return version;
        }

        public static Optional<ApplicationProtocol> find( ApplicationProtocolCategory category, Integer version )
        {
            return Protocol.find( ApplicationProtocols.values(), category, version, Function.identity() );
        }
    }

    interface ModifierProtocol extends Protocol<String>
    {
    }

    enum ModifierProtocolCategory implements Category<ModifierProtocol>
    {
        COMPRESSION,
        // Need a second Category for testing purposes.
        GRATUITOUS_OBFUSCATION;

        @Override
        public String canonicalName()
        {
            return name().toLowerCase();
        }
    }

    enum ModifierProtocols implements ModifierProtocol
    {
        COMPRESSION_GZIP( ModifierProtocolCategory.COMPRESSION, Implementations.GZIP ),
        COMPRESSION_SNAPPY( ModifierProtocolCategory.COMPRESSION, Implementations.SNAPPY ),
        COMPRESSION_SNAPPY_VALIDATING( ModifierProtocolCategory.COMPRESSION, Implementations.SNAPPY_VALIDATING ),
        COMPRESSION_LZ4( ModifierProtocolCategory.COMPRESSION, Implementations.LZ4 ),
        COMPRESSION_LZ4_HIGH_COMPRESSION( ModifierProtocolCategory.COMPRESSION, Implementations.LZ4_HIGH_COMPRESSION ),
        COMPRESSION_LZ4_VALIDATING( ModifierProtocolCategory.COMPRESSION, Implementations.LZ_VALIDATING ),
        COMPRESSION_LZ4_HIGH_COMPRESSION_VALIDATING( ModifierProtocolCategory.COMPRESSION, Implementations.LZ4_HIGH_COMPRESSION_VALIDATING );

        // Should be human writable into a comma separated list
        private final String friendlyName;
        private final ModifierProtocolCategory identifier;

        ModifierProtocols( ModifierProtocolCategory identifier, String friendlyName )
        {
            this.identifier = identifier;
            this.friendlyName = friendlyName;
        }

        @Override
        public String implementation()
        {
            return friendlyName;
        }

        @Override
        public String category()
        {
            return identifier.canonicalName();
        }

        public static Optional<ModifierProtocol> find( ModifierProtocolCategory category, String friendlyName )
        {
            return Protocol.find( ModifierProtocols.values(), category, friendlyName, String::toLowerCase );
        }

        public static class Implementations
        {
            public static final String GZIP = "Gzip";
            public static final String SNAPPY = "Snappy";
            public static final String SNAPPY_VALIDATING = "Snappy_validating";
            public static final String LZ4 = "LZ4";
            public static final String LZ4_HIGH_COMPRESSION = "LZ4_high_compression";
            public static final String LZ_VALIDATING = "LZ_validating";
            public static final String LZ4_HIGH_COMPRESSION_VALIDATING = "LZ4_high_compression_validating";
        }
    }
}
