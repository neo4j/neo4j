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
import java.util.OptionalLong;
import java.util.Set;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.TopologyGraphDbmsModel.DatabaseAccess;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * An extension of {@link DatabaseInfo} with the additional fields for the information returned by {@link DatabaseInfoService#requestDetailedInfo(Set)}.
 */
public class ExtendedDatabaseInfo extends DatabaseInfo {
    public static final long COMMITTED_TX_ID_NOT_AVAILABLE = -1;

    private final long lastCommittedTxId;
    private final long txCommitLag;
    private final boolean committedTxIdNotAvailable;

    /**
     * If the lastCommittedTxId is set to COMMITTED_TX_ID_NOT_AVAILABLE both lastCommittedTxId() and txCommitLag() will return empty optionals
     */
    public ExtendedDatabaseInfo(
            NamedDatabaseId namedDatabaseId,
            ServerId serverId,
            DatabaseAccess accessFromConfig,
            SocketAddress boltAddress,
            SocketAddress catchupAddress,
            String role,
            String status,
            String error,
            long lastCommittedTxId,
            long txCommitLag) {
        super(namedDatabaseId, serverId, accessFromConfig, boltAddress, catchupAddress, role, status, error);
        this.committedTxIdNotAvailable = lastCommittedTxId == COMMITTED_TX_ID_NOT_AVAILABLE;
        this.lastCommittedTxId = committedTxIdNotAvailable ? 0 : lastCommittedTxId;
        this.txCommitLag = committedTxIdNotAvailable ? 0 : txCommitLag;
    }

    /**
     * Returns the last committed transaction id for this database on this server. If the last
     * committed transaction id is not available empty is return.
     *
     * OptionalLong.empty() is used instead of -1 to indicate "not available" to be consistent with
     * {@link #txCommitLag()} where -1 is a valid return value.
     *
     * @return last committed transaction id or empty if not available
     */
    public OptionalLong lastCommittedTxId() {
        return committedTxIdNotAvailable ? OptionalLong.empty() : OptionalLong.of(lastCommittedTxId);
    }

    /**
     * Returns the difference between the maximum known committed transaction of this database and the
     * committed transaction id of this server. A lag of 0 indicate that this server is up to date while
     * a negative value indicate how many transactions behind this server is.
     *
     * If last committed transaction id is not available the commit lag in not applicable and
     * empty is returned.
     *
     * @return the lag in number of committed transactions
     */
    public OptionalLong txCommitLag() {
        return committedTxIdNotAvailable ? OptionalLong.empty() : OptionalLong.of(txCommitLag);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExtendedDatabaseInfo that = (ExtendedDatabaseInfo) o;
        return lastCommittedTxId == that.lastCommittedTxId
                && txCommitLag == that.txCommitLag
                && Objects.equals(namedDatabaseId, that.namedDatabaseId)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(access, that.access)
                && Objects.equals(boltAddress, that.boltAddress)
                && Objects.equals(catchupAddress, that.catchupAddress)
                && Objects.equals(role, that.role)
                && Objects.equals(status, that.status)
                && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                namedDatabaseId,
                serverId,
                access,
                boltAddress,
                catchupAddress,
                role,
                status,
                error,
                lastCommittedTxId,
                txCommitLag);
    }

    @Override
    public String toString() {
        return "ExtendedDatabaseInfoImpl{" + "namedDatabaseId="
                + namedDatabaseId + ", serverId="
                + serverId + ", accessFromConfig="
                + access + ", boltAddress="
                + boltAddress + ", catchupAddress="
                + catchupAddress + ", role='"
                + role + '\'' + ", status='"
                + status + '\'' + ", error='"
                + error + '\'' + ", lastCommittedTxId="
                + lastCommittedTxId() + ", txCommitLag="
                + txCommitLag() + '}';
    }
}
