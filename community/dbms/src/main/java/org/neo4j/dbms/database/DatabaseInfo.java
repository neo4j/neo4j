/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.dbms.database;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.TopologyGraphDbmsModel.DatabaseAccess;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * A wrapper class of the information returned by {@link DatabaseInfoService#lookupCachedInfo(Set)}
 */
public class DatabaseInfo
{
    final NamedDatabaseId namedDatabaseId;
    final ServerId serverId;
    final DatabaseAccess access;
    final SocketAddress boltAddress;
    final SocketAddress catchupAddress;
    final String role;
    final String status;
    final String error;

    public DatabaseInfo( NamedDatabaseId namedDatabaseId, ServerId serverId, DatabaseAccess access, SocketAddress boltAddress,
                         SocketAddress catchupAddress, String role, String status, String error )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.serverId = serverId;
        this.access = access;
        this.boltAddress = boltAddress;
        this.catchupAddress = catchupAddress;
        this.role = role;
        this.status = status;
        this.error = error;
    }

    public ExtendedDatabaseInfo extendWith( long lastCommittedTxId )
    {
        return this.extendWith( lastCommittedTxId, lastCommittedTxId );
    }

    public ExtendedDatabaseInfo extendWith( long lastCommittedTxId, long maxCommittedTxId )
    {
        return new ExtendedDatabaseInfo( namedDatabaseId, serverId, access, boltAddress, catchupAddress, role, status, error, lastCommittedTxId,
                                             lastCommittedTxId - maxCommittedTxId );
    }

    public NamedDatabaseId namedDatabaseId()
    {
        return namedDatabaseId;
    }

    /**
     * @return the serverId of the server or empty if server does not have a ServerId
     */
    public Optional<ServerId> serverId()
    {
        return Optional.ofNullable( serverId );
    }

    public ServerId rawServerId()
    {
        return serverId;
    }

    public DatabaseAccess access()
    {
        return access;
    }

    /**
     * @return the address of the server or empty if address is not currently known
     */
    public Optional<SocketAddress> boltAddress()
    {
        return Optional.ofNullable( boltAddress );
    }

    public Optional<SocketAddress> catchupAddress()
    {
        return Optional.ofNullable( catchupAddress);
    }

    public String role()
    {
        return role;
    }

    public String status()
    {
        return status;
    }

    public String error()
    {
        return error;
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
        DatabaseInfo that = (DatabaseInfo) o;
        return Objects.equals( namedDatabaseId, that.namedDatabaseId ) &&
               Objects.equals( serverId, that.serverId ) &&
               Objects.equals( access, that.access ) &&
               Objects.equals( boltAddress, that.boltAddress ) &&
               Objects.equals( catchupAddress, that.catchupAddress ) &&
               Objects.equals( role, that.role ) &&
               Objects.equals( status, that.status ) &&
               Objects.equals( error, that.error );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( namedDatabaseId, serverId, access, boltAddress, catchupAddress, role, status, error );
    }

    @Override
    public String toString()
    {
        return "DatabaseInfoImpl{" +
               "namedDatabaseId=" + namedDatabaseId +
               ", serverId=" + serverId +
               ", accessFromConfig=" + access +
               ", boltAddress=" + boltAddress +
               ", catchupAddress=" + catchupAddress +
               ", role='" + role + '\'' +
               ", status='" + status + '\'' +
               ", error='" + error + '\'' +
               '}';
    }
}
