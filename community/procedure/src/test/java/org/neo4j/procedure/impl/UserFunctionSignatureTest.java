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
package org.neo4j.procedure.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;

class UserFunctionSignatureTest {
    private final UserFunctionSignature signature = functionSignature(new QualifiedName("asd"))
            .in("in", Neo4jTypes.NTAny)
            .out(Neo4jTypes.NTAny)
            .build();

    @Test
    void inputSignatureShouldNotBeModifiable() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> signature.inputSignature().add(FieldSignature.inputField("in2", Neo4jTypes.NTAny)));
    }

    @Test
    void toStringShouldMatchCypherSyntax() {
        // When
        String toStr = functionSignature(new QualifiedName("org", "myProcedure"))
                .in("in", Neo4jTypes.NTList(Neo4jTypes.NTString))
                .out(Neo4jTypes.NTNumber)
                .build()
                .toString();

        // Then
        assertEquals("org.myProcedure(in :: LIST<STRING>) :: INTEGER | FLOAT", toStr);
    }
}
