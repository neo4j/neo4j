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
package org.neo4j.kernel.impl.transaction.stats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.test.Race;

class DatabaseTransactionStatsTest {
    @Test
    void shouldSeeTransactionsFromWReadToWriteAsActive() {
        // given
        var stats = new DatabaseTransactionStats();
        stats.transactionStarted();

        // when
        var race = new Race().withEndCondition(() -> false);
        race.addContestant(stats::upgradeToWriteTransaction, 1);
        race.addContestant(
                () -> {
                    var numActiveTransactions = stats.getNumberOfActiveTransactions();
                    assertThat(numActiveTransactions).isGreaterThan(0);
                },
                1);
        race.goUnchecked();
    }
}
