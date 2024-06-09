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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;

@SuppressWarnings("WeakerAccess")
public class ProcedureSignatureTest {
    private static final QualifiedName PROCEDURE_NAME = new QualifiedName("org", "myProcedure");
    private static final ProcedureSignature signature =
            procedureSignature(PROCEDURE_NAME).in("a", Neo4jTypes.NTAny).build();

    @Test
    void inputSignatureShouldNotBeModifiable() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> signature.inputSignature().add(FieldSignature.inputField("b", Neo4jTypes.NTAny)));
    }

    @Test
    void outputSignatureShouldNotBeModifiable() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> signature.outputSignature().add(FieldSignature.outputField("b", Neo4jTypes.NTAny)));
    }

    @Test
    void shouldHonorVoidInEquals() {
        ProcedureSignature sig1 =
                procedureSignature(PROCEDURE_NAME).in("a", Neo4jTypes.NTAny).build();
        ProcedureSignature sig2 = procedureSignature(PROCEDURE_NAME)
                .in("a", Neo4jTypes.NTAny)
                .out(ProcedureSignature.VOID)
                .build();
        ProcedureSignature sig2clone = procedureSignature(PROCEDURE_NAME)
                .in("a", Neo4jTypes.NTAny)
                .out(ProcedureSignature.VOID)
                .build();

        assertEquals(sig2, sig2clone);
        assertNotEquals(sig1, sig2);
    }

    @Test
    void toStringShouldMatchCypherSyntax() {
        // When
        String toStr = procedureSignature(PROCEDURE_NAME)
                .in("inputArg", Neo4jTypes.NTList(Neo4jTypes.NTString))
                .out("outputArg", Neo4jTypes.NTNumber)
                .build()
                .toString();

        // Then
        assertEquals("org.myProcedure(inputArg :: LIST<STRING>) :: (outputArg :: INTEGER | FLOAT)", toStr);
    }

    @Test
    void toStringForVoidProcedureShouldMatchCypherSyntax() {
        // Given
        ProcedureSignature proc = procedureSignature(PROCEDURE_NAME)
                .in("inputArg", Neo4jTypes.NTList(Neo4jTypes.NTString))
                .out(ProcedureSignature.VOID)
                .build();

        // When
        String toStr = proc.toString();

        // Then
        assertEquals("org.myProcedure(inputArg :: LIST<STRING>)", toStr);
    }
}
