/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.systemgraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_SEED_RESTORE_UNTIL_PROPERTY;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.graphdb.Node;

public class SeedRestoreUntilTest {

    @Test
    void shouldThrowWhenTransactionIdInvalid() {
        // given
        long invalidTxId = -100;

        // when/then
        assertThatThrownBy(() -> SeedRestoreUntil.validateArgs(OptionalLong.of(invalidTxId), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        String.format("Transaction id should be a positive number. Provided value: %s", invalidTxId));
    }

    @Test
    void shouldThrowWhenNoRestoreUntilProvided() {
        // given
        var txId = OptionalLong.empty();
        var datetime = Optional.<ZonedDateTime>empty();

        // when/then
        assertThatThrownBy(() -> SeedRestoreUntil.validateArgs(txId, datetime))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Must contain either a transaction id or transaction date");
    }

    @Test
    void shouldThrowWhenBothTransactionIdAndDateProvided() {
        // given
        var txId = OptionalLong.of(100L);
        var datetime = Optional.of(ZonedDateTime.now());

        // when/then
        assertThatThrownBy(() -> SeedRestoreUntil.validateArgs(txId, datetime))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Only one of transaction id or transaction date can be provided");
    }

    @Test
    void shouldConvertFromValidObject() {
        // given
        Object txId = 100L;

        // when
        var seedRestoreUntil1 = SeedRestoreUntil.fromObj(txId);

        // then
        assertThat(seedRestoreUntil1.txId().getAsLong()).isEqualTo(100L);

        // given
        var instant = Instant.now();
        var zoneId = ZoneId.systemDefault();
        Object datetime = ZonedDateTime.ofInstant(instant, zoneId);

        // when
        var seedRestoreUntil2 = SeedRestoreUntil.fromObj(datetime);

        // then
        assertThat(seedRestoreUntil2.dateTime.orElseThrow()).isEqualTo(ZonedDateTime.ofInstant(instant, zoneId));
    }

    @Test
    void shouldThrowWhenConvertingFromInvalidObject() {
        // given
        var invalidType = "invalidType";

        // when/then
        assertThatThrownBy(() -> SeedRestoreUntil.fromObj(invalidType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Provided value can't be converted to transaction id or transaction date");
    }

    @Test
    void shouldWritePropertyToDatabaseNode() {
        // given
        var database = Mockito.mock(Node.class);
        var seedRestoreUntil = SeedRestoreUntil.datetime(ZonedDateTime.now());

        // when
        seedRestoreUntil.writeProperty(database);

        // then
        verify(database, times(1))
                .setProperty(DATABASE_SEED_RESTORE_UNTIL_PROPERTY, seedRestoreUntil.dateTime.orElseThrow());
    }

    @Test
    void shouldDisplayValue() {
        // given
        var datetime = ZonedDateTime.now();
        var seedRestoreUntil1 = SeedRestoreUntil.datetime(datetime);
        var seedRestoreUntil2 = SeedRestoreUntil.txId(10L);

        // when/then
        assertThat(seedRestoreUntil1.toOptionValue()).isEqualTo(datetime.toString());
        assertThat(seedRestoreUntil2.toOptionValue()).isEqualTo("10");
    }
}
