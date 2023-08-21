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
package org.neo4j.kernel.api.impl.schema;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.api.impl.schema.AbstractTextIndexProvider.UPDATE_IGNORE_STRATEGY;
import static org.neo4j.kernel.api.impl.schema.LuceneTestTokenNameLookup.SIMPLE_TOKEN_LOOKUP;

import java.lang.reflect.InvocationHandler;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.annotations.documented.ReporterFactories;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.ValueIndexReader;

class TextIndexAccessorTest {
    @SuppressWarnings("unchecked")
    private final DatabaseIndex<ValueIndexReader> schemaIndex =
            (DatabaseIndex<ValueIndexReader>) mock(DatabaseIndex.class);

    private TextIndexAccessor accessor;

    @BeforeEach
    void setUp() {
        accessor = new TextIndexAccessor(
                schemaIndex,
                IndexPrototype.forSchema(forLabel(1, 2)).withName("a").materialise(1),
                SIMPLE_TOKEN_LOOKUP,
                UPDATE_IGNORE_STRATEGY);
    }

    @Test
    void indexIsNotConsistentWhenIndexIsNotValid() {
        when(schemaIndex.isValid()).thenReturn(false);
        assertFalse(accessor.consistencyCheck(ReporterFactories.noopReporterFactory(), NULL_CONTEXT_FACTORY, 1));
    }

    @Test
    void indexIsConsistentWhenIndexIsValid() {
        when(schemaIndex.isValid()).thenReturn(true);
        assertTrue(accessor.consistencyCheck(ReporterFactories.noopReporterFactory(), NULL_CONTEXT_FACTORY, 1));
    }

    @Test
    void indexReportInconsistencyToVisitor() {
        when(schemaIndex.isValid()).thenReturn(false);
        MutableBoolean called = new MutableBoolean();
        final InvocationHandler handler = (proxy, method, args) -> {
            called.setTrue();
            return null;
        };
        assertFalse(
                accessor.consistencyCheck(new ReporterFactory(handler), NULL_CONTEXT_FACTORY, 1),
                "Expected index to be inconsistent");
        assertTrue(called.booleanValue(), "Expected visitor to be called");
    }
}
