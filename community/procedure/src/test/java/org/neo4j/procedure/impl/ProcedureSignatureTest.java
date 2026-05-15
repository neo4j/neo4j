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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;

class ProcedureSignatureTest {
    private static final QualifiedName PROCEDURE_NAME = new QualifiedName("org", "myProcedure");
    private static final ProcedureSignature signature =
            procedureSignature(PROCEDURE_NAME).in("a", Neo4jTypes.NTAny).build();

    @Test
    void inputSignatureShouldNotBeModifiable() {
        assertThatThrownBy(() -> signature.inputSignature().add(FieldSignature.inputField("b", Neo4jTypes.NTAny)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void outputSignatureShouldNotBeModifiable() {
        assertThatThrownBy(() -> signature.outputSignature().add(FieldSignature.outputField("b", Neo4jTypes.NTAny)))
                .isInstanceOf(UnsupportedOperationException.class);
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

        assertThat(sig2clone).isEqualTo(sig2);
        assertThat(sig2).isNotEqualTo(sig1);
    }

    @Test
    void toStringShouldMatchCypherSyntax() {
        // When
        ProcedureSignature signature = procedureSignature(PROCEDURE_NAME)
                .in("inputArg", Neo4jTypes.NTList(Neo4jTypes.NTString))
                .out("outputArg", Neo4jTypes.NTNumber)
                .build();

        // Then
        assertThat(signature)
                .hasToString("org.myProcedure(inputArg :: LIST<STRING>) :: (outputArg :: INTEGER | FLOAT)");
    }

    @Test
    void toStringForVoidProcedureShouldMatchCypherSyntax() {
        // Given
        ProcedureSignature proc = procedureSignature(PROCEDURE_NAME)
                .in("inputArg", Neo4jTypes.NTList(Neo4jTypes.NTString))
                .out(ProcedureSignature.VOID)
                .build();

        // Then
        assertThat(proc).hasToString("org.myProcedure(inputArg :: LIST<STRING>)");
    }
}
