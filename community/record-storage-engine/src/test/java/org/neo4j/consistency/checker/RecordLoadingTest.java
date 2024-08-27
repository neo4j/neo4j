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
package org.neo4j.consistency.checker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.api.TokenHolder;

class RecordLoadingTest {
    @Test
    void shouldReturnsFalseOnMissingToken() {
        // given
        NodeRecord entity = new NodeRecord(0);
        TokenHolder tokenHolder = new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, "Test");
        TokenStore<PropertyKeyTokenRecord> store = mock(TokenStore.class);
        BiConsumer noopReporter = mock(BiConsumer.class);

        // when
        boolean valid = RecordLoading.checkValidToken(
                entity,
                0,
                tokenHolder,
                store,
                noopReporter,
                noopReporter,
                StoreCursors.NULL,
                EmptyMemoryTracker.INSTANCE);

        // then
        assertFalse(valid);
    }
}
