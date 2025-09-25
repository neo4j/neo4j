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
package org.neo4j.internal.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.neo4j.internal.id.IdUtils.combinedIdAndNumberOfIds;

import java.util.Arrays;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class IdUtilsTest {
    @Inject
    private RandomSupport random;

    @RepeatedTest(100)
    void shouldCombineIdAndNumberOfIds() {
        // given
        long id = random.nextLong(IdUtils.MAX_ID + 1);
        int numberOfIds = random.intBetween(1, IdUtils.MAX_NUMBER_OF_IDS);
        boolean used = random.nextBoolean();

        // when
        long combinedId = combinedIdAndNumberOfIds(id, numberOfIds, used);

        // then
        assertThat(IdUtils.idFromCombinedId(combinedId)).isEqualTo(id);
        assertThat(IdUtils.numberOfIdsFromCombinedId(combinedId)).isEqualTo(numberOfIds);
        assertThat(IdUtils.usedFromCombinedId(combinedId)).isEqualTo(used);
    }

    @Test
    void testExtremes() {
        // max values
        long combinedId = combinedIdAndNumberOfIds(IdUtils.MAX_ID, IdUtils.MAX_NUMBER_OF_IDS, true);

        assertThat(IdUtils.idFromCombinedId(combinedId)).isEqualTo(IdUtils.MAX_ID);
        assertThat(IdUtils.numberOfIdsFromCombinedId(combinedId)).isEqualTo(IdUtils.MAX_NUMBER_OF_IDS);
        assertThat(IdUtils.usedFromCombinedId(combinedId)).isEqualTo(true);

        // min values
        combinedId = combinedIdAndNumberOfIds(0, 1, false);

        assertThat(IdUtils.idFromCombinedId(combinedId)).isEqualTo(0);
        assertThat(IdUtils.numberOfIdsFromCombinedId(combinedId)).isEqualTo(1);
        assertThat(IdUtils.usedFromCombinedId(combinedId)).isEqualTo(false);

        // bounds checks
        assertThatCode(() -> combinedIdAndNumberOfIds(-1, 1, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ID ");
        assertThatCode(() -> combinedIdAndNumberOfIds(IdUtils.MAX_ID + 1, 1, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ID ");

        assertThatCode(() -> combinedIdAndNumberOfIds(0, 0, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Number of IDs ");
        assertThatCode(() -> combinedIdAndNumberOfIds(0, IdUtils.MAX_NUMBER_OF_IDS + 1, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Number of IDs ");
    }

    @Test
    void sortOnId() {
        int n = 1000;
        long[] combinedArray = new long[n];
        for (int i = 0; i < n; i++) {
            combinedArray[i] = combinedIdAndNumberOfIds(random.nextLong(IdUtils.MAX_ID + 1), 1, false);
        }

        Arrays.sort(combinedArray);

        long previousId = -1;
        for (long combined : combinedArray) {
            long id = IdUtils.idFromCombinedId(combined);
            assertThat(id).isGreaterThanOrEqualTo(previousId);
            previousId = id;
        }
    }
}
