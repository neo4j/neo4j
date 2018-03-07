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

public interface Protocol
{
    String identifier();

    int version();

    interface Identifier<T extends Protocol>
    {
        String canonicalName();
    }

    interface ApplicationProtocol extends Protocol
    {
    }

    enum ApplicationProtocolIdentifier implements Identifier<ApplicationProtocol>
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
        RAFT_1( ApplicationProtocolIdentifier.RAFT, 1 ),
        CATCHUP_1( ApplicationProtocolIdentifier.CATCHUP, 1 );

        private final int version;
        private final ApplicationProtocolIdentifier identifier;

        ApplicationProtocols( ApplicationProtocolIdentifier identifier, int version )
        {
            this.identifier = identifier;
            this.version = version;
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
    }

    interface ModifierProtocol extends Protocol
    {
        /**
         * Should be human readable in a comma separated list
         */
        String friendlyName();
    }

    enum ModifierProtocolIdentifier implements Identifier<ModifierProtocol>
    {
        COMPRESSION,
        GRATUITOUS_OBFUSCATION;

        @Override
        public String canonicalName()
        {
            return name().toLowerCase();
        }
    }

    enum ModifierProtocols implements ModifierProtocol
    {
        COMPRESSION_SNAPPY( ModifierProtocolIdentifier.COMPRESSION, 1, "Snappy" );

        private final int version;
        private final ModifierProtocolIdentifier identifier;
        private final String friendlyName;

        ModifierProtocols( ModifierProtocolIdentifier identifier, int version, String friendlyName )
        {
            this.version = version;
            this.identifier = identifier;
            this.friendlyName = friendlyName;
        }

        @Override
        public int version()
        {
            return version;
        }

        @Override
        public String identifier()
        {
            return identifier.canonicalName();
        }

        @Override
        public String friendlyName()
        {
            return friendlyName;
        }
    }
}
