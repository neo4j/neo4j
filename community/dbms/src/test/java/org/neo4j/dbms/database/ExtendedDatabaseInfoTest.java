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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.dbms.database.ExtendedDatabaseInfo.COMMITTED_TX_ID_NOT_AVAILABLE;

import org.junit.jupiter.api.Test;
import org.neo4j.storageengine.api.StoreId;

class ExtendedDatabaseInfoTest {
    @Test
    void shouldReturnEmptyLastCommittedTxId() {
        // given
        var databaseInfo = new ExtendedDatabaseInfo(
                null, null, null, null, null, null, false, null, null, COMMITTED_TX_ID_NOT_AVAILABLE, -1, null, 1, 0);

        // when
        var result = databaseInfo.lastCommittedTxId();

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnEmptyTxCommitLag() {
        // given
        var databaseInfo = new ExtendedDatabaseInfo(
                null, null, null, null, null, null, false, null, null, COMMITTED_TX_ID_NOT_AVAILABLE, -42, null, 1, 0);

        // when
        var result = databaseInfo.txCommitLag();

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnEmptyStoreId() {
        // given
        var databaseInfo = new ExtendedDatabaseInfo(
                null, null, null, null, null, null, false, null, null, 3, -42, StoreId.UNKNOWN, 1, 0);

        // when
        var result = databaseInfo.storeId();

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnDetailedDbInfoWithValue() {
        // given
        var expectedLastCommittedTxId = 5040;
        var expectedStoreId = new StoreId(1, 1, "engine", "format", 1, 1);
        var databaseInfo = new ExtendedDatabaseInfo(
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                null,
                null,
                expectedLastCommittedTxId,
                -42,
                expectedStoreId,
                1,
                0);

        // when
        var actual_lastCommittedTxId = databaseInfo.lastCommittedTxId();
        var actual_storeId = databaseInfo.storeId();

        // then
        assertThat(actual_lastCommittedTxId).hasValue(expectedLastCommittedTxId);
        assertThat(actual_storeId).hasValue(expectedStoreId.getStoreVersionUserString());
    }

    @Test
    void shouldReturnTxCommitLagWithValue() {
        // given
        var txCommitLag = -1;
        var databaseInfo = new ExtendedDatabaseInfo(
                null, null, null, null, null, null, false, null, null, 5040, txCommitLag, null, 1, 0);

        // when
        var result = databaseInfo.txCommitLag();

        // then
        assertThat(result).hasValue(txCommitLag);
    }

    @Test
    void shouldBeEqualIfConstructedWithDifferentTxCommitLagButNoCommittedTxIdAvailable() {
        // given
        var databaseInfo1 = new ExtendedDatabaseInfo(
                null, null, null, null, null, null, false, null, null, -1, -7, StoreId.UNKNOWN, 1, 0);
        var databaseInfo2 = new ExtendedDatabaseInfo(
                null, null, null, null, null, null, false, null, null, -1, -50, StoreId.UNKNOWN, 1, 0);

        // then
        assertThat(databaseInfo1).isEqualTo(databaseInfo2);
    }
}
