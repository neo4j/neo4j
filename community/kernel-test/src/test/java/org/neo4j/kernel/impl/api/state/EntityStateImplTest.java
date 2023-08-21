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
package org.neo4j.kernel.impl.api.state;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.factory.OnHeapCollectionsFactory;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Values;

class EntityStateImplTest {
    @Test
    void shouldListAddedProperties() {
        // Given
        EntityStateImpl state = new EntityStateImpl(1, OnHeapCollectionsFactory.INSTANCE, EmptyMemoryTracker.INSTANCE);
        state.addProperty(1, Values.of("Hello"));
        state.addProperty(2, Values.of("Hello"));
        state.removeProperty(1);

        // When
        Iterator<StorageProperty> added = state.addedProperties().iterator();

        // Then
        assertThat(Iterators.asList(added)).isEqualTo(asList(new PropertyKeyValue(2, Values.of("Hello"))));
    }

    @Test
    void shouldListAddedPropertiesEvenIfPropertiesHaveBeenReplaced() {
        // Given
        EntityStateImpl state = new EntityStateImpl(1, OnHeapCollectionsFactory.INSTANCE, EmptyMemoryTracker.INSTANCE);
        state.addProperty(1, Values.of("Hello"));
        state.addProperty(1, Values.of("WAT"));
        state.addProperty(2, Values.of("Hello"));

        // When
        Iterator<StorageProperty> added = state.addedProperties().iterator();

        // Then
        assertThat(Iterators.asList(added))
                .isEqualTo(
                        asList(new PropertyKeyValue(1, Values.of("WAT")), new PropertyKeyValue(2, Values.of("Hello"))));
    }

    @Test
    void shouldConvertAddRemoveToChange() {
        // Given
        EntityStateImpl state = new EntityStateImpl(1, OnHeapCollectionsFactory.INSTANCE, EmptyMemoryTracker.INSTANCE);

        // When
        state.removeProperty(4);
        state.addProperty(4, Values.of("another value"));

        // Then
        assertThat(Iterators.asList(state.changedProperties().iterator()))
                .isEqualTo(asList(new PropertyKeyValue(4, Values.of("another value"))));
        assertFalse(state.addedProperties().iterator().hasNext());
        assertTrue(state.removedProperties().isEmpty());
    }
}
