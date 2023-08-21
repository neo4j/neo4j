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

import java.util.Arrays;
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
        var selection = PropertySelection.selection(ids);

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
}
