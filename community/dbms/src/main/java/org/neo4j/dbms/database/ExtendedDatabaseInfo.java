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
import java.util.OptionalLong;
import java.util.Set;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.TopologyGraphDbmsModel.DatabaseAccess;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.storageengine.api.StoreId;

/**
 * An extension of {@link DatabaseInfo} with the additional fields for the information returned by {@link DatabaseInfoService#requestDetailedInfo(Set)}.
 */
public class ExtendedDatabaseInfo extends DatabaseInfo {
    public static final long COMMITTED_TX_ID_NOT_AVAILABLE = -1;

    private final long lastCommittedTxId;
    private final long txCommitLag;
    private final boolean committedTxIdNotAvailable;
    private final StoreId storeId;
    private final int primariesCount;
    private final int secondariesCount;

    /**
     * If the lastCommittedTxId is set to COMMITTED_TX_ID_NOT_AVAILABLE both lastCommittedTxId() and txCommitLag() will return empty optionals
     * If StoreId is UNKNOWN, storeId() will return an empty optional.
     */
    public ExtendedDatabaseInfo(
            NamedDatabaseId namedDatabaseId,
            ServerId serverId,
            DatabaseAccess accessFromConfig,
            SocketAddress boltAddress,
            SocketAddress catchupAddress,
            String oldRole,
            String role,
            boolean writer,
            String status,
            String statusMessage,
            long lastCommittedTxId,
            long txCommitLag,
            StoreId storeId,
            int primariesCount,
            int secondariesCount) {
        super(
                namedDatabaseId,
                serverId,
                accessFromConfig,
                boltAddress,
                catchupAddress,
                oldRole,
                role,
                writer,
                status,
                statusMessage);
        this.committedTxIdNotAvailable = lastCommittedTxId == COMMITTED_TX_ID_NOT_AVAILABLE;
        this.lastCommittedTxId = committedTxIdNotAvailable ? 0 : lastCommittedTxId;
        this.txCommitLag = committedTxIdNotAvailable ? 0 : txCommitLag;
        this.storeId = storeId;
        this.primariesCount = primariesCount;
        this.secondariesCount = secondariesCount;
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

    /**
     * Returns empty optional if storeId is UNKNOWN.
     * @return the string representation of the storeId for a certain db on a certain server
     */
    public Optional<String> storeId() {
        return storeId.equals(StoreId.UNKNOWN) ? Optional.empty() : Optional.of(storeId.getStoreVersionUserString());
    }

    public int primariesCount() {
        return primariesCount;
    }

    public int secondariesCount() {
        return secondariesCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ExtendedDatabaseInfo that = (ExtendedDatabaseInfo) o;
        return lastCommittedTxId == that.lastCommittedTxId
                && txCommitLag == that.txCommitLag
                && committedTxIdNotAvailable == that.committedTxIdNotAvailable
                && primariesCount == that.primariesCount
                && secondariesCount == that.secondariesCount
                && Objects.equals(storeId, that.storeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                lastCommittedTxId,
                txCommitLag,
                committedTxIdNotAvailable,
                storeId,
                primariesCount,
                secondariesCount);
    }

    @Override
    public String toString() {
        return "ExtendedDatabaseInfo{" + "lastCommittedTxId="
                + lastCommittedTxId + ", txCommitLag="
                + txCommitLag + ", committedTxIdNotAvailable="
                + committedTxIdNotAvailable + ", storeId="
                + storeId + ", primariesCount="
                + primariesCount + ", secondariesCount="
                + secondariesCount + ", namedDatabaseId="
                + namedDatabaseId + ", serverId="
                + serverId + ", access="
                + access + ", boltAddress="
                + boltAddress + ", catchupAddress="
                + catchupAddress + ", oldRole='"
                + oldRole + '\'' + ", role='"
                + role + '\'' + ", writer="
                + writer + ", status='"
                + status + '\'' + ", statusMessage='"
                + statusMessage + '\'' + '}';
    }
}
