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
package org.neo4j.kernel.impl.index.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class DefaultTokenIndexIdLayoutTest {

    @Inject
    private RandomSupport random;

    @Test
    void testBasicConversionFromEntityId() {
        var idLayout = new DefaultTokenIndexIdLayout();

        for (int i = 0; i < 1000; i++) {
            var entityId = random.nextLong(0, Long.MAX_VALUE);
            assertThat(idLayout.rangeOf(entityId)).isEqualTo(entityId / TokenScanValue.RANGE_SIZE);
            assertThat(idLayout.idWithinRange(entityId)).isEqualTo(entityId % TokenScanValue.RANGE_SIZE);
        }
    }

    @Test
    void testBasicConversionToEntityId() {
        var idLayout = new DefaultTokenIndexIdLayout();
        for (int i = 0; i < 1000; i++) {
            var range = random.nextLong();
            assertThat(idLayout.firstIdOfRange(range)).isEqualTo(range * TokenScanValue.RANGE_SIZE);
        }
    }

    @Test
    void testEdgeCases() {
        var idLayout = new DefaultTokenIndexIdLayout();
        // full range seeks should start from the lowest actual range that is 0
        assertThat(idLayout.rangeOf(Long.MIN_VALUE)).isLessThan(0);
        // range seeks add 1 to cover to-exclusive entity id, full range shouldn't fail with overflow
        assertThat(Math.addExact(idLayout.rangeOf(Long.MAX_VALUE), 1)).isGreaterThan(0);
    }
}
