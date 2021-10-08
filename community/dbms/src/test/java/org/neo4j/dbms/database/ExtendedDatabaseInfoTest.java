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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.dbms.database.ExtendedDatabaseInfo.COMMITTED_TX_ID_NOT_AVAILABLE;

class ExtendedDatabaseInfoTest
{
    @Test
    void shouldReturnEmptyLastCommittedTxId()
    {
        // given
        var databaseInfo = new ExtendedDatabaseInfo( null, null, null, null, null, null, null, null, COMMITTED_TX_ID_NOT_AVAILABLE, -1 );

        // when
        var result = databaseInfo.lastCommittedTxId();

        // then
        assertThat( result ).isEmpty();
    }

    @Test
    void shouldReturnEmptyTxCommitLag()
    {
        // given
        var databaseInfo = new ExtendedDatabaseInfo( null, null, null, null, null, null, null, null, COMMITTED_TX_ID_NOT_AVAILABLE, -42 );

        // when
        var result = databaseInfo.txCommitLag();

        // then
        assertThat( result ).isEmpty();
    }

    @Test
    void shouldReturnLastCommittedTxIdWithValue()
    {
        // given
        var lastCommittedTxId = 5040;
        var databaseInfo = new ExtendedDatabaseInfo( null, null, null, null, null, null, null, null, lastCommittedTxId, -42 );

        // when
        var result = databaseInfo.lastCommittedTxId();

        // then
        assertThat( result ).hasValue( lastCommittedTxId );
    }

    @Test
    void shouldReturnTxCommitLagWithValue()
    {
        // given
        var txCommitLag = -1;
        var databaseInfo = new ExtendedDatabaseInfo( null, null, null, null, null, null, null, null, 5040, txCommitLag );

        // when
        var result = databaseInfo.txCommitLag();

        // then
        assertThat( result ).hasValue( txCommitLag );
    }

    @Test
    void shouldBeEqualIfConstructedWithDifferentTxCommitLagButNoCommittedTxIdAvailable()
    {
        // given
        var databaseInfo1 = new ExtendedDatabaseInfo( null, null, null, null, null, null, null, null, -1, -7 );
        var databaseInfo2 = new ExtendedDatabaseInfo( null, null, null, null, null, null, null, null, -1, -50 );

        // then
        assertThat( databaseInfo1 ).isEqualTo( databaseInfo2 );
    }
}
