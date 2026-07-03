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
package org.neo4j.storageengine.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
class HighestAppendBatchTest {

    @Inject
    RandomSupport randomSupport;

    @Test
    void shouldHardSetHighest() {
        // GIVEN
        HighestAppendBatch highest = new HighestAppendBatch(new AppendBatchInfo(12, LogPosition.UNSPECIFIED));

        // WHEN
        highest.set(1, new LogPosition(1, 28));

        // THEN
        assertThat(highest.get()).isEqualTo(new AppendBatchInfo(1, new LogPosition(1, 28)));
    }

    @Test
    void shouldOnlyKeepTheHighestOffered() {
        // GIVEN
        HighestAppendBatch highest = new HighestAppendBatch(new AppendBatchInfo(-1, LogPosition.UNSPECIFIED));

        // WHEN/THEN
        assertAccepted(highest, 2);
        assertAccepted(highest, 5);
        assertRejected(highest, 3);
        assertRejected(highest, 4);
        assertAccepted(highest, 10);
    }

    @Test
    void shouldKeepHighestDuringConcurrentOfferings() throws Throwable {
        HighestAppendBatch highestAppendBatch = new HighestAppendBatch(new AppendBatchInfo(1, LogPosition.UNSPECIFIED));
        Race race = new Race();

        race.addContestants(100, () -> {
            for (int i = 0; i < 100000; i++) {
                var update = randomSupport.random().nextLong();
                highestAppendBatch.offer(update, LogPosition.UNSPECIFIED);
                assertThat(highestAppendBatch.get().appendIndex()).isGreaterThanOrEqualTo(update);
            }
        });

        race.go();
    }

    private static void assertAccepted(HighestAppendBatch highest, long appendIndex) {
        AppendBatchInfo current = highest.get();
        highest.offer(appendIndex, LogPosition.UNSPECIFIED);
        assertThat(highest.get().appendIndex()).isEqualTo(appendIndex);
        assertThat(appendIndex).isGreaterThan(current.appendIndex());
    }

    private static void assertRejected(HighestAppendBatch highest, long txId) {
        AppendBatchInfo current = highest.get();
        highest.offer(txId, LogPosition.UNSPECIFIED);
        assertThat(highest.get()).isEqualTo(current);
    }
}
