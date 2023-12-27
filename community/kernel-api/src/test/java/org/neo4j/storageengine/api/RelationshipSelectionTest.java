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

import static org.apache.commons.lang3.ArrayUtils.shuffle;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class RelationshipSelectionTest {
    @Inject
    private RandomSupport random;

    @Test
    void shouldProvideHighestTypeForMultiTypeSelection() {
        // given
        var typesReference = new int[] {0, 1, 2, 3, 4, 5};
        var types = typesReference.clone();
        do {
            shuffle(types, random.random());
        } while (Arrays.equals(typesReference, types));

        // when
        var selection = RelationshipSelection.selection(types, Direction.OUTGOING);
        Arrays.fill(types, -1);

        // then
        assertThat(selection.highestType()).isEqualTo(typesReference[typesReference.length - 1]);
    }
}
