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
package org.neo4j.storageengine.api;

import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;
import static org.neo4j.storageengine.api.PropertySelection.selection;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class PropertySelectionTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldSortKeysInMultiKeySelection() {
        // given
        var tokens = new int[50];
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = i;
        }
        var ids = random.selection(tokens, 2, tokens.length, false);

        // when
        var selection = selection(ids);

        // then
        assertThat(selection.numberOfKeys()).isEqualTo(ids.length);
        var idsFromSelection = new int[selection.numberOfKeys()];
        for (int i = 0; i < selection.numberOfKeys(); i++) {
            idsFromSelection[i] = selection.key(i);
            if (i > 0) {
                assertThat(idsFromSelection[i]).isGreaterThan(idsFromSelection[i - 1]);
            }
        }
        Arrays.sort(ids);
        assertThat(idsFromSelection).isEqualTo(ids);
    }

    @Test
    void shouldExcludeKeyFromSingleKeySelection() {
        // given
        var key = random.nextInt(1_000);
        var selection = selection(key);

        // when
        var filteredSelection = selection.excluding(k -> k == key);
        var unfilteredSelection = selection.excluding(k -> false);

        // then
        assertThat(filteredSelection.test(key)).isFalse();
        assertThat(unfilteredSelection.test(key)).isTrue();
        assertThat(unfilteredSelection).isSameAs(selection);
    }

    @Test
    void shouldExcludeKeysFromMultiKeySelection() {
        // given
        var keys = random.selection(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 10, false);
        var selection = selection(keys);

        // when
        var keysToExclude = random.selection(keys, 1, keys.length, false);
        Arrays.sort(keysToExclude);
        var filteredSelection = selection.excluding(k -> ArrayUtils.contains(keysToExclude, k));

        // then
        for (int key : keys) {
            assertThat(filteredSelection.test(key)).isEqualTo(!contains(keysToExclude, key));
        }
    }

    @Test
    void shouldExcludeKeysFromAllKeySelection() {
        // given
        var selection = ALL_PROPERTIES;

        // when
        var keysToExclude = random.selection(new int[] {0, 1, 2, 3, 4, 5}, 1, 6, false);
        Arrays.sort(keysToExclude);
        var filteredSelection = selection.excluding(k -> ArrayUtils.contains(keysToExclude, k));

        // then
        for (int key = 0; key < 100; key++) {
            assertThat(filteredSelection.test(key)).isEqualTo(!contains(keysToExclude, key));
        }
    }

    @Test
    void shouldFilterOutNullTokens() {
        // given
        var keys = random.selection(new int[] {0, 1, 2, 3, 4, 5}, 1, 6, false);
        var keysSprinkledWithNulls = sprinkleWithNullTokens(keys);

        // when
        var selection = PropertySelection.selection(keysSprinkledWithNulls);

        // then
        assertThat(selection.lowestKey()).isEqualTo(IntStream.of(keys).min().getAsInt());
        assertThat(selection.highestKey()).isEqualTo(IntStream.of(keys).max().getAsInt());
    }

    private int[] sprinkleWithNullTokens(int[] keys) {
        var result = IntLists.mutable.empty();
        for (int key : keys) {
            if (random.nextBoolean()) {
                result.add(NO_TOKEN);
            }
            result.add(key);
            if (random.nextBoolean()) {
                result.add(NO_TOKEN);
            }
        }
        return result.toArray();
    }
}
