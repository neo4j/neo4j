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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.NoSuchElementException;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.procs.QualifiedName;

class ProcedureHolderTest {
    @Test
    void shouldGetProcedureFromHolder() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qualifiedName = new QualifiedName(new String[0], "CaseSensitive");
        String item = "CaseSensitiveItem";
        procHolder.put(qualifiedName, item, false);

        // then
        assertThat(procHolder.get(qualifiedName)).isEqualTo(item);
        assertThat(procHolder.idOf(qualifiedName)).isEqualTo(0);
    }

    @Test
    void okToHaveProcsOnlyDifferByCase() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        procHolder.put(new QualifiedName(new String[0], "CASESENSITIVE"), "CASESENSITIVEItem", false);
        procHolder.put(new QualifiedName(new String[0], "CaseSensitive"), "CaseSensitiveItem", false);

        // then
        assertThat(procHolder.get(new QualifiedName(new String[0], "CASESENSITIVE")))
                .isEqualTo("CASESENSITIVEItem");
        assertThat(procHolder.get(new QualifiedName(new String[0], "CaseSensitive")))
                .isEqualTo("CaseSensitiveItem");
        assertThat(procHolder.idOf(new QualifiedName(new String[0], "CASESENSITIVE")))
                .isEqualTo(0);
        assertThat(procHolder.idOf(new QualifiedName(new String[0], "CaseSensitive")))
                .isEqualTo(1);
    }

    @Test
    void shouldGetCaseInsensitiveFromHolder() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qualifiedName = new QualifiedName(new String[0], "CaseInSensitive");
        String item = "CaseInSensitiveItem";
        procHolder.put(qualifiedName, item, true);

        // then
        QualifiedName lowerCaseName = new QualifiedName(new String[0], "caseinsensitive");
        assertThat(procHolder.get(lowerCaseName)).isEqualTo(item);
        assertThat(procHolder.idOf(lowerCaseName)).isEqualTo(0);
    }

    @Test
    void canOverwriteFunctionAndChangeCaseSensitivity() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qualifiedName = new QualifiedName(new String[0], "CaseInSensitive");
        String item = "CaseInSensitiveItem";
        procHolder.put(qualifiedName, item, true);

        // then
        QualifiedName lowerCaseName = new QualifiedName(new String[0], "caseinsensitive");
        assertThat(procHolder.get(lowerCaseName)).isEqualTo(item);
        assertThat(procHolder.idOf(lowerCaseName)).isEqualTo(0);

        // and then
        procHolder.put(qualifiedName, item, false);
        assertNull(procHolder.get(lowerCaseName));
        assertThrows(NoSuchElementException.class, () -> procHolder.idOf(lowerCaseName));
    }

    @Test
    void preservesIdsForUnregisteredItems() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName(new String[0], "CaseInSensitive");
        int id = procHolder.put(qn, "value", true);

        // when
        procHolder.unregister(qn);
        procHolder.put(qn, "value", true);

        // then
        assertThat(procHolder.idOf(qn)).isEqualTo(id);
    }

    @Test
    void tombstoneProcedureHolderPreservesRequested() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName(new String[0], "CaseInSensitive");
        String item = "CaseInSensitiveItem";

        int id = procHolder.put(qn, item, true);

        // when
        var renewed = ProcedureHolder.tombstone(procHolder, Set.of(id));

        // then
        assertThat(renewed.get(qn)).isEqualTo(item);
        assertThat(renewed.get(id)).isEqualTo(item);
        assertThat(renewed.idOf(qn)).isEqualTo(id);
    }

    @Test
    void tombstoneProcedureHolderRemovesOther() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName(new String[0], "CaseInSensitive");
        String item = "CaseInSensitiveItem";
        int id = procHolder.put(qn, item, true);

        // when
        var renewed = ProcedureHolder.tombstone(procHolder, Set.of());

        // then
        assertNull(renewed.get(qn));
        assertNull(renewed.get(id));
        assertThatThrownBy(() -> renewed.idOf(qn)).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void tombstoneProcedureHolderPreservesIdsAndNamesForRestoredEntries() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName(new String[0], "CaseInSensitive");
        QualifiedName qn2 = new QualifiedName(new String[0], "qn2");
        String item = "CaseInSensitiveItem";
        int removedId = procHolder.put(qn, item, true), keptId = procHolder.put(qn2, item, true);

        // when
        var renewed = ProcedureHolder.tombstone(procHolder, Set.of(keptId));
        renewed.put(qn, item, true);
        renewed.put(qn2, item, true);

        // then
        assertThat(renewed.get(qn)).isEqualTo(item);
        assertThat(renewed.idOf(qn)).isEqualTo(removedId);
        assertThat(renewed.get(qn2)).isEqualTo(item);
        assertThat(renewed.idOf(qn2)).isEqualTo(keptId);
    }
}
