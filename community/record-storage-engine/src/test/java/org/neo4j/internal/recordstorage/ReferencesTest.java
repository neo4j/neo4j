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
package org.neo4j.internal.recordstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.recordstorage.RelationshipReferenceEncoding.DENSE;
import static org.neo4j.internal.recordstorage.RelationshipReferenceEncoding.clearEncoding;
import static org.neo4j.internal.recordstorage.RelationshipReferenceEncoding.parseEncoding;

import java.util.concurrent.ThreadLocalRandom;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.junit.jupiter.api.Test;
import org.neo4j.storageengine.api.LongReference;

class ReferencesTest {
    // This value the largest possible high limit id +1 (see HighLimitV3_1_0)
    private static final long MAX_ID_LIMIT = 1L << 50;

    @Test
    void shouldPreserveNoId() {
        assertThat(RelationshipReferenceEncoding.encodeDense(LongReference.NULL))
                .isEqualTo(LongReference.NULL);
    }

    @Test
    void shouldClearFlags() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 1000; i++) {
            long reference = random.nextLong(MAX_ID_LIMIT);
            int token = random.nextInt(Integer.MAX_VALUE);

            assertThat(clearEncoding(RelationshipReferenceEncoding.encodeDense(reference)))
                    .isEqualTo(reference);
        }
    }

    @Test
    void encodeDense() {
        testLongFlag(DENSE, RelationshipReferenceEncoding::encodeDense);
    }

    private static void testLongFlag(RelationshipReferenceEncoding flag, LongToLongFunction encoder) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 1000; i++) {
            long reference = random.nextLong(MAX_ID_LIMIT);
            assertNotEquals(flag, parseEncoding(reference));
            assertEquals(flag, parseEncoding(encoder.applyAsLong(reference)));
            assertTrue(encoder.applyAsLong(reference) < 0, "encoded reference is negative");
        }
    }
}
