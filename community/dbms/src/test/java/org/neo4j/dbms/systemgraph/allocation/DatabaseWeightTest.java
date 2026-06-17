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
package org.neo4j.dbms.systemgraph.allocation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class DatabaseWeightTest {

    @Test
    public void testConstructorValidation() {
        // given
        var validWeights = List.of(1, 97, 200000, Integer.MAX_VALUE, 0);
        var invalidWeights = List.of(Integer.MIN_VALUE, -1000, -7);

        // when/then
        for (var validWeight : validWeights) {
            assertThatCode(() -> new DatabaseWeight(validWeight)).doesNotThrowAnyException();
        }

        for (var invalidWeight : invalidWeights) {
            assertThatThrownBy(() -> new DatabaseWeight(invalidWeight)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testComparable() {
        // given
        var dw1 = new DatabaseWeight(5);
        var dw2 = new DatabaseWeight(10);
        var dw3 = new DatabaseWeight(1);
        var dw4 = new DatabaseWeight(0);

        var weights = new ArrayList<>(List.of(dw1, dw2, dw3, dw4));

        // when you sort the list
        Collections.sort(weights);

        // then it should be in ascending order of weight
        assertThat(weights).isEqualTo(List.of(dw4, dw3, dw1, dw2));
    }
}
