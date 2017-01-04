/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.query.clientsession;

public abstract class ClientSessionInfo
{
    /**
     * Used by {@link #asConnectionDetails()} only. When the {@code connectionDetails} string is no longer needed,
     * this can go away, since the username is provided though other means to the places that need it.
     */
    @Deprecated
    public ClientSessionInfo withUsername( String username )
    {
        return new SessionInfoWithUsername( this, username );
    }

    @Deprecated
    public abstract String asConnectionDetails();

    public String requestScheme()
    {
        return null;
    }

    public String clientAddress()
    {
        return null;
    }

    public String requestURI()
    {
        return null;
    }

    public static final ClientSessionInfo EMBEDDED_SESSION = new ClientSessionInfo()
    {
        @Override
        public String asConnectionDetails()
        {
            return "embedded-session\t";
        }
    };

    @Deprecated
    private static class SessionInfoWithUsername extends ClientSessionInfo
    {
        private final ClientSessionInfo source;
        private final String username;

        private SessionInfoWithUsername( ClientSessionInfo source, String username )
        {
            this.source = source;
            this.username = username;
        }

        @Override
        public String asConnectionDetails()
        {
            return source.asConnectionDetails() + '\t' + username;
        }

        @Override
        public String requestScheme()
        {
            return source.requestScheme();
        }

        @Override
        public String clientAddress()
        {
            return source.clientAddress();
        }

        @Override
        public String requestURI()
        {
            return source.requestURI();
        }
    }
}
