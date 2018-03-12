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

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public interface Protocol<IMPL extends Comparable<IMPL>>
{
    String category();

    IMPL implementation();

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
        COMPRESSION_GZIP( ModifierProtocolCategory.COMPRESSION, "Gzip" ),
        COMPRESSION_SNAPPY( ModifierProtocolCategory.COMPRESSION, "Snappy" ),
        COMPRESSION_SNAPPY_VALIDATING( ModifierProtocolCategory.COMPRESSION, "Snappy_validating" ),
        COMPRESSION_LZ4( ModifierProtocolCategory.COMPRESSION, "LZ4" ),
        COMPRESSION_LZ4_HIGH_COMPRESSION( ModifierProtocolCategory.COMPRESSION, "LZ4_high_compression" ),
        COMPRESSION_LZ4_VALIDATING( ModifierProtocolCategory.COMPRESSION, "LZ_validating" ),
        COMPRESSION_LZ4_HIGH_COMPRESSION_VALIDATING( ModifierProtocolCategory.COMPRESSION, "LZ4_high_compression_validating" );

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

        public static Optional<ModifierProtocols> fromFriendlyName( String friendlyName )
        {
            return Stream.of( ModifierProtocols.values() )
                    .filter( protocol -> Objects.equals( protocol.friendlyName.toLowerCase(), friendlyName.toLowerCase() ) )
                    .findFirst();
        }
    }
}
