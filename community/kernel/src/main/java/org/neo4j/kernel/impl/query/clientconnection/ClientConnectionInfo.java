/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.query.clientconnection;

/**
 * This is implemented as an abstract class in order to support different formatting for {@link #asConnectionDetails()},
 * when this method is no longer needed, and we move to a standardized format across all types of connections, we can
 * turn this class into a simpler value type that just holds the fields that are actually used.
 */
public abstract class ClientConnectionInfo
{
    /**
     * Used by {@link #asConnectionDetails()} only. When the {@code connectionDetails} string is no longer needed,
     * this can go away, since the username is provided though other means to the places that need it.
     */
    @Deprecated
    public ClientConnectionInfo withUsername( String username )
    {
        return new ConnectionInfoWithUsername( this, username );
    }

    /**
     * This method provides the custom format for each type of connection.
     * <p>
     * Preferably we would not need to have a custom format for each type of connection, but this is provided for
     * backwards compatibility reasons.
     *
     * @return a custom log-line format describing this type of connection.
     */
    @Deprecated
    public abstract String asConnectionDetails();

    /**
     * Which protocol was used for this connection.
     * <p>
     * This is not necessarily an internet protocol (like http et.c.) although it could be. It might also be "embedded"
     * for example, if this connection represents an embedded session.
     *
     * @return the protocol used for connecting to the server.
     */
    public abstract String protocol();

    /**
     * This method is overridden in the subclasses where this information is available.
     *
     * @return the address of the client. or {@code null} if the address is not available.
     */
    public String clientAddress()
    {
        return null;
    }

    /**
     * This method is overridden in the subclasses where this information is available.
     *
     * @return the URI of this server that the client connected to, or {@code null} if the URI is not available.
     */
    public String requestURI()
    {
        return null;
    }

    public static final ClientConnectionInfo EMBEDDED_CONNECTION = new ClientConnectionInfo()
    {
        @Override
        public String asConnectionDetails()
        {
            return "embedded-session\t";
        }

        @Override
        public String protocol()
        {
            return "embedded";
        }
    };

    /**
     * Should be removed along with {@link #withUsername(String)} and {@link #asConnectionDetails()}.
     */
    @Deprecated
    private static class ConnectionInfoWithUsername extends ClientConnectionInfo
    {
        private final ClientConnectionInfo source;
        private final String username;

        private ConnectionInfoWithUsername( ClientConnectionInfo source, String username )
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
        public String protocol()
        {
            return source.protocol();
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
