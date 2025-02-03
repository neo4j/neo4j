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
package org.neo4j.procedure.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;

/**
 * Tests for {@link ListComponentsProcedure}.
 */
class ListComponentsProcedureTest {
    @Test
    void usesFalseSemanticVersionWhenNotOverridden() throws ProcedureException {
        // Given
        var procedure = new ListComponentsProcedure(
                new QualifiedName(new String[] {"dbms"}, "components"), "2025.01.0", "community", false);

        // When
        try (var result = procedure.apply(null, new AnyValue[0], null)) {
            // Then
            assertTrue(result.hasNext(), "Expected a result row from the procedure.");

            var row = result.next();
            assertEquals("Neo4j Kernel", ((TextValue) row[0]).stringValue());
            var versions = (ListValue) row[1];
            assertEquals(1, versions.intSize());
            assertEquals("5.27.0", ((TextValue) versions.value(0)).stringValue());
            assertEquals("community", ((TextValue) row[2]).stringValue());

            assertFalse(result.hasNext(), "Expected only one row from the procedure.");
        }
    }

    @Test
    void usesCustomVersionWhenOverridden() throws ProcedureException {
        // Given
        var procedure = new ListComponentsProcedure(
                new QualifiedName(new String[] {"dbms"}, "components"), "banana", "enterprise", true);

        // When
        try (var result = procedure.apply(null, new AnyValue[0], null)) {
            // Then
            assertTrue(result.hasNext(), "Expected a result row from the procedure.");

            var row = result.next();

            assertEquals("Neo4j Kernel", ((TextValue) row[0]).stringValue());
            var versions = (ListValue) row[1];
            assertEquals(1, versions.intSize());
            assertEquals("banana", ((TextValue) versions.value(0)).stringValue());
            assertEquals("enterprise", ((TextValue) row[2]).stringValue());

            assertFalse(result.hasNext(), "Expected only one row from the procedure.");
        }
    }
}
