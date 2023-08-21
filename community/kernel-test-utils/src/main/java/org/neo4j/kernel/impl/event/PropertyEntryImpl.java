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
package org.neo4j.kernel.impl.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.internal.helpers.Strings;
import org.neo4j.values.storable.Values;

class PropertyEntryImpl<T> implements PropertyEntry<T> {
    private final T entity;
    private final String key;
    private final Object value;
    private final Object valueBeforeTx;

    PropertyEntryImpl(T entity, String key, Object value, Object valueBeforeTx) {
        this.entity = entity;
        this.key = key;
        this.value = value;
        this.valueBeforeTx = valueBeforeTx;
    }

    @Override
    public T entity() {
        return this.entity;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public Object value() {
        return this.value;
    }

    @Override
    public Object previouslyCommittedValue() {
        return this.valueBeforeTx;
    }

    void compareToAssigned(PropertyEntry<T> entry) {
        basicCompareTo(entry);
        assertEqualsMaybeNull(entry.value(), value(), entry.entity(), entry.key());
    }

    void compareToRemoved(PropertyEntry<T> entry) {
        basicCompareTo(entry);
        try {
            entry.value();
            fail("Should throw IllegalStateException");
        } catch (IllegalStateException e) {
            // OK
        }
        assertNull(value());
    }

    private void basicCompareTo(PropertyEntry<T> entry) {
        assertEquals(entry.entity(), entity());
        assertEquals(entry.key(), key());
        assertEqualsMaybeNull(
                entry.previouslyCommittedValue(), previouslyCommittedValue(), entry.entity(), entry.key());
    }

    @Override
    public String toString() {
        return "PropertyEntry[entity=" + entity + ", key=" + key + ", value=" + value + ", valueBeforeTx="
                + valueBeforeTx + "]";
    }

    private static <T> void assertEqualsMaybeNull(Object o1, Object o2, T entity, String key) {
        String entityDescription = "For " + entity + " and " + key;
        if (o1 == null || o2 == null) {
            assertSame(o1, o2, entityDescription + ". " + Strings.prettyPrint(o1) + " != " + Strings.prettyPrint(o2));
        } else {
            assertEquals(Values.of(o1), Values.of(o2), entityDescription);
        }
    }
}
