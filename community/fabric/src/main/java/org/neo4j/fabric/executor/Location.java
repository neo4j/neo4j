/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.executor;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.configuration.helpers.SocketAddress;

public class Location
{
    private final long graphId;
    private final UUID uuid;
    private final String databaseName;

    private Location( long graphId, UUID uuid, String databaseName )
    {
        this.graphId = graphId;
        this.uuid = uuid;
        this.databaseName = databaseName;
    }

    public long getGraphId()
    {
        return graphId;
    }

    public UUID getUuid()
    {
        return uuid;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public static class Local extends Location
    {
        public Local( long id, UUID uuid, String databaseName )
        {
            super( id, uuid, databaseName );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Local local = (Local) o;
            return Objects.equals( getGraphId(), local.getGraphId() )
                    && Objects.equals( getUuid(), local.getUuid() )
                    && Objects.equals( getDatabaseName(), local.getDatabaseName() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( getGraphId(), getUuid(), getDatabaseName() );
        }

        @Override
        public String toString()
        {
            return "Local{" + "graphId=" + getGraphId() + ", uuid=" + getUuid() + ", databaseName='" + getDatabaseName() + '\'' + '}';
        }
    }

    public abstract static class Remote extends Location
    {
        private final RemoteUri uri;

        protected Remote( long id, UUID uuid, RemoteUri uri, String databaseName )
        {
            super( id, uuid, databaseName );
            this.uri = uri;
        }

        public RemoteUri getUri()
        {
            return uri;
        }

        public static class Internal extends Remote
        {

            public Internal( long id, UUID uuid, RemoteUri uri, String databaseName )
            {
                super( id, uuid, uri, databaseName );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                Remote remote = (Remote) o;
                return getUri().equals( remote.getUri() ) || Objects.equals( getGraphId(), remote.getGraphId() )
                        && Objects.equals( getUuid(), remote.getUuid() )
                        && Objects.equals( getDatabaseName(), remote.getDatabaseName() );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( getUri(), getGraphId(), getUuid(), getDatabaseName() );
            }

            @Override
            public String toString()
            {
                return "Internal{" + "graphId=" + getGraphId() + ", uuid=" + getUuid() + ", databaseName='" + getDatabaseName() + '\'' + ", uri=" + getUri() +
                        '}';
            }
        }

        public static class External extends Remote
        {

            public External( long id, UUID uuid, RemoteUri uri, String databaseName )
            {
                super( id, uuid, uri, databaseName );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                Remote remote = (Remote) o;
                return getUri().equals( remote.getUri() ) || Objects.equals( getGraphId(), remote.getGraphId() )
                        && Objects.equals( getUuid(), remote.getUuid() )
                        && Objects.equals( getDatabaseName(), remote.getDatabaseName() );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( getUri(), getGraphId(), getUuid(), getDatabaseName() );
            }

            @Override
            public String toString()
            {
                return "External{" + "graphId=" + getGraphId() + ", uuid=" + getUuid() + ", databaseName='" + getDatabaseName() + '\'' + ", uri=" + getUri() +
                        '}';
            }
        }
    }

    public static class RemoteUri
    {
        private final String scheme;
        private final List<SocketAddress> addresses;
        private final String query;

        public RemoteUri( String scheme, List<SocketAddress> addresses, String query )
        {
            this.scheme = scheme;
            this.addresses = addresses;
            this.query = query;
        }

        public String getScheme()
        {
            return scheme;
        }

        public List<SocketAddress> getAddresses()
        {
            return addresses;
        }

        public String getQuery()
        {
            return query;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            RemoteUri remoteUri = (RemoteUri) o;
            return Objects.equals( scheme, remoteUri.scheme ) && Objects.equals( addresses, remoteUri.addresses ) && Objects.equals( query, remoteUri.query );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( scheme, addresses, query );
        }

        @Override
        public String toString()
        {
            return "RemoteUri{" + "scheme='" + scheme + '\'' + ", addresses=" + addresses + ", query='" + query + '\'' + '}';
        }
    }
}
