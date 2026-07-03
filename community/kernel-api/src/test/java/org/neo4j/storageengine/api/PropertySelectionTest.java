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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;
import static org.neo4j.storageengine.api.PropertySelection.selection;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

import java.util.Arrays;
import java.util.stream.IntStream;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
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
        var filteredSelection = selection.excluding(key);
        var unfilteredSelection = selection.excluding();

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
        var filteredSelection = selection.excluding(keysToExclude);

        // then
        for (int key : keys) {
            assertThat(filteredSelection.test(key))
                    .isEqualTo(IntStream.of(keysToExclude).noneMatch(k -> k == key));
        }
    }

    @Test
    void shouldExcludeKeysFromAllExceptionSelection() {
        var keysToExclude0 = random.selection(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 4, false);
        var selection = PropertySelection.ALL_PROPERTIES.excluding(keysToExclude0);

        var keysToExclude1 = random.selection(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 2, 4, false);
        var filteredSelection = selection.excluding(keysToExclude1);

        // then
        for (int key : new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
            assertThat(filteredSelection.test(key))
                    .isEqualTo(IntStream.of(keysToExclude0).noneMatch(k -> k == key)
                            && IntStream.of(keysToExclude1).noneMatch(k -> k == key));
        }
    }

    @Test
    void shouldExcludeKeysFromAllKeySelection() {
        // given
        var selection = ALL_PROPERTIES;

        // when
        var keysToExclude = random.selection(new int[] {0, 1, 2, 3, 4, 5}, 1, 6, false);
        var filteredSelection = selection.excluding(keysToExclude);

        // then
        for (int key = 0; key < 100; key++) {
            final int currentKey = key;
            assertThat(filteredSelection.test(currentKey))
                    .isEqualTo(IntStream.of(keysToExclude).noneMatch(k -> k == currentKey));
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

    @Test
    void shouldSelectSpecificValuesToInclude() {
        // given
        var keys = random.selection(new int[] {0, 1, 2, 3, 4, 5}, 1, 6, false);
        var values = IntSets.immutable.of(random.selection(keys, 1, keys.length, false));

        // when
        var selection = PropertySelection.selection(values::contains, keys);

        // then
        for (int key : keys) {
            assertThat(selection.test(key)).isTrue();
            assertThat(selection.includeValue(key)).isEqualTo(values.contains(key));
        }
    }

    @Test
    void testKeysAreGuaranteedSubsetOf() {
        var none = PropertySelection.NO_PROPERTIES;

        var single = PropertySelection.selection(1);

        var multiple = PropertySelection.selection(1, 2, 3);
        var multiple2 = PropertySelection.selection(1, 2, 3, 4);
        var multiple3 = PropertySelection.selection(3, 4);

        var allExcept2 = PropertySelection.ALL_PROPERTIES.excluding(2);
        var allExcept3 = PropertySelection.ALL_PROPERTIES.excluding(3);
        var allExcept2_3 = PropertySelection.ALL_PROPERTIES.excluding(2, 3);

        var allProps = PropertySelection.ALL_PROPERTIES;

        assertThat(none.keysAreGuaranteedSubsetOf(none)).isTrue();
        assertThat(none.keysAreGuaranteedSubsetOf(single)).isTrue();
        assertThat(none.keysAreGuaranteedSubsetOf(multiple)).isTrue();
        assertThat(none.keysAreGuaranteedSubsetOf(allProps)).isTrue();
        assertThat(none.keysAreGuaranteedSubsetOf(allExcept2)).isTrue();

        assertThat(single.keysAreGuaranteedSubsetOf(none)).isFalse();
        assertThat(single.keysAreGuaranteedSubsetOf(single)).isTrue();
        assertThat(single.keysAreGuaranteedSubsetOf(multiple)).isTrue();
        assertThat(single.keysAreGuaranteedSubsetOf(multiple2)).isTrue();
        assertThat(single.keysAreGuaranteedSubsetOf(multiple3)).isFalse();
        assertThat(single.keysAreGuaranteedSubsetOf(allExcept2)).isTrue();
        assertThat(single.keysAreGuaranteedSubsetOf(allProps)).isTrue();

        assertThat(multiple.keysAreGuaranteedSubsetOf(none)).isFalse();
        assertThat(multiple.keysAreGuaranteedSubsetOf(single)).isFalse();
        assertThat(multiple.keysAreGuaranteedSubsetOf(multiple)).isTrue();
        assertThat(multiple.keysAreGuaranteedSubsetOf(multiple2)).isTrue();
        assertThat(multiple.keysAreGuaranteedSubsetOf(multiple3)).isFalse();
        assertThat(multiple.keysAreGuaranteedSubsetOf(allExcept2)).isFalse();
        assertThat(multiple.keysAreGuaranteedSubsetOf(allProps)).isTrue();

        assertThat(multiple3.keysAreGuaranteedSubsetOf(allExcept2)).isTrue();

        assertThat(allExcept2.keysAreGuaranteedSubsetOf(none)).isFalse();
        assertThat(allExcept2.keysAreGuaranteedSubsetOf(single)).isFalse();
        assertThat(allExcept2.keysAreGuaranteedSubsetOf(multiple)).isFalse();
        assertThat(allExcept2.keysAreGuaranteedSubsetOf(allExcept2)).isTrue();
        assertThat(allExcept2.keysAreGuaranteedSubsetOf(allExcept3)).isFalse();
        assertThat(allExcept2.keysAreGuaranteedSubsetOf(allExcept2_3)).isFalse();
        assertThat(allExcept2.keysAreGuaranteedSubsetOf(allProps)).isTrue();

        assertThat(allExcept3.keysAreGuaranteedSubsetOf(allExcept2)).isFalse();
        assertThat(allExcept3.keysAreGuaranteedSubsetOf(allExcept3)).isTrue();
        assertThat(allExcept3.keysAreGuaranteedSubsetOf(allExcept2_3)).isFalse();

        assertThat(allExcept2_3.keysAreGuaranteedSubsetOf(allExcept2)).isTrue();
        assertThat(allExcept2_3.keysAreGuaranteedSubsetOf(allExcept3)).isTrue();
        assertThat(allExcept2_3.keysAreGuaranteedSubsetOf(allExcept2_3)).isTrue();

        assertThat(allProps.keysAreGuaranteedSubsetOf(none)).isFalse();
        assertThat(allProps.keysAreGuaranteedSubsetOf(single)).isFalse();
        assertThat(allProps.keysAreGuaranteedSubsetOf(multiple)).isFalse();
        assertThat(allProps.keysAreGuaranteedSubsetOf(allExcept2)).isFalse();
        assertThat(allProps.keysAreGuaranteedSubsetOf(allProps)).isTrue();
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
