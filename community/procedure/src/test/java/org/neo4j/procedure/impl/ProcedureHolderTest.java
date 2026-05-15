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

import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.function.Predicates;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.QueryLanguage;

class ProcedureHolderTest {
    @Test
    void shouldGetProcedureFromHolder() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qualifiedName = new QualifiedName("CaseSensitive");
        String item = "CaseSensitiveItem";
        procHolder.put(qualifiedName, QueryLanguage.ALL, item, false);

        // then
        assertThat(procHolder.getByKey(qualifiedName, QueryLanguage.CYPHER_5)).isEqualTo(item);
        assertThat(procHolder.idOfKey(qualifiedName, QueryLanguage.CYPHER_5)).isEqualTo(0);
    }

    @Test
    void okToHaveProcsOnlyDifferByCase() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        procHolder.put(new QualifiedName("CASESENSITIVE"), QueryLanguage.ALL, "CASESENSITIVEItem", false);
        procHolder.put(new QualifiedName("CaseSensitive"), QueryLanguage.ALL, "CaseSensitiveItem", false);

        // then
        assertThat(procHolder.getByKey(new QualifiedName("CASESENSITIVE"), QueryLanguage.CYPHER_5))
                .isEqualTo("CASESENSITIVEItem");
        assertThat(procHolder.getByKey(new QualifiedName("CaseSensitive"), QueryLanguage.CYPHER_5))
                .isEqualTo("CaseSensitiveItem");
        assertThat(procHolder.idOfKey(new QualifiedName("CASESENSITIVE"), QueryLanguage.CYPHER_5))
                .isEqualTo(0);
        assertThat(procHolder.idOfKey(new QualifiedName("CaseSensitive"), QueryLanguage.CYPHER_5))
                .isEqualTo(1);
    }

    @Test
    void shouldGetCaseInsensitiveFromHolder() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qualifiedName = new QualifiedName("CaseInSensitive");
        String item = "CaseInSensitiveItem";
        procHolder.put(qualifiedName, QueryLanguage.ALL, item, true);

        // then
        QualifiedName lowerCaseName = new QualifiedName("caseinsensitive");
        assertThat(procHolder.getByKey(lowerCaseName, QueryLanguage.CYPHER_5)).isEqualTo(item);
        assertThat(procHolder.idOfKey(lowerCaseName, QueryLanguage.CYPHER_5)).isEqualTo(0);
    }

    @Test
    void canOverwriteFunctionAndChangeCaseSensitivity() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qualifiedName = new QualifiedName("CaseInSensitive");
        String item = "CaseInSensitiveItem";
        procHolder.put(qualifiedName, QueryLanguage.ALL, item, true);

        // then
        QualifiedName lowerCaseName = new QualifiedName("caseinsensitive");
        assertThat(procHolder.getByKey(lowerCaseName, QueryLanguage.CYPHER_5)).isEqualTo(item);
        assertThat(procHolder.idOfKey(lowerCaseName, QueryLanguage.CYPHER_5)).isEqualTo(0);

        // and then
        procHolder.put(qualifiedName, QueryLanguage.ALL, item, false);
        assertThat(procHolder.getByKey(lowerCaseName, QueryLanguage.CYPHER_5)).isNull();
        assertThatThrownBy(() -> procHolder.idOfKey(lowerCaseName, QueryLanguage.CYPHER_5))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void preservesIdsForUnregisteredItems() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName("CaseInSensitive");
        int id = procHolder.put(qn, QueryLanguage.ALL, "value", true);

        // when
        procHolder.unregister(qn);
        procHolder.put(qn, QueryLanguage.ALL, "value", true);

        // then
        assertThat(procHolder.idOfKey(qn, QueryLanguage.CYPHER_5)).isEqualTo(id);
    }

    @Test
    void tombstoneProcedureHolderPreservesRequested() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName("CaseInSensitive");
        String item = "CaseInSensitiveItem";

        int id = procHolder.put(qn, QueryLanguage.ALL, item, true);

        // when
        var renewed = ProcedureHolder.tombstone(procHolder, Predicates.alwaysFalse());

        // then
        assertThat(renewed.getById(id)).isEqualTo(item);
        for (var scope : QueryLanguage.values()) {
            assertThat(renewed.getByKey(qn, scope)).isEqualTo(item);
            assertThat(renewed.idOfKey(qn, scope)).isEqualTo(id);
        }
    }

    @Test
    void tombstoneProcedureHolderRemovesOther() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName("CaseInSensitive");
        String item = "CaseInSensitiveItem";
        int id = procHolder.put(qn, QueryLanguage.ALL, item, true);

        // when
        var renewed = ProcedureHolder.tombstone(procHolder, Predicates.alwaysTrue());

        // then
        assertThat(renewed.getById(id)).isNull();
        for (var scope : QueryLanguage.values()) {
            assertThat(renewed.getByKey(qn, scope)).isNull();
            assertThatThrownBy(() -> renewed.idOfKey(qn, scope)).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void tombstoneProcedureHolderPreservesIdsAndNamesForRestoredEntries() {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName("CaseInSensitive");
        QualifiedName qn2 = new QualifiedName("qn2");
        String item = "CaseInSensitiveItem";
        int removedId = procHolder.put(qn, QueryLanguage.ALL, item, true),
                keptId = procHolder.put(qn2, QueryLanguage.ALL, item, true);

        // when
        var renewed = ProcedureHolder.tombstone(procHolder, (qual) -> qual.equals(qn2));
        renewed.put(qn, QueryLanguage.ALL, item, true);
        renewed.put(qn2, QueryLanguage.ALL, item, true);

        // then
        assertThat(renewed.getByKey(qn, QueryLanguage.CYPHER_5)).isEqualTo(item);
        assertThat(renewed.idOfKey(qn, QueryLanguage.CYPHER_5)).isEqualTo(removedId);
        assertThat(renewed.getByKey(qn2, QueryLanguage.CYPHER_5)).isEqualTo(item);
        assertThat(renewed.idOfKey(qn2, QueryLanguage.CYPHER_5)).isEqualTo(keptId);
    }

    @Test
    void canAddSeparateScopes() {
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName("qn");
        procHolder.put(qn, Set.of(QueryLanguage.CYPHER_5), "left", false);
        procHolder.put(qn, Set.of(QueryLanguage.CYPHER_25), "right", false);
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_5)).isEqualTo("left");
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_25)).isEqualTo("right");
    }

    @Test
    void canAddJointScope() {
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName("qn");
        procHolder.put(qn, QueryLanguage.ALL, "both", false);
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_5)).isEqualTo("both");
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_25)).isEqualTo("both");
    }

    @Test
    void canUpdateToJointScope() {
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName("qn");
        procHolder.put(qn, Set.of(QueryLanguage.CYPHER_5), "left", false);
        procHolder.put(qn, Set.of(QueryLanguage.CYPHER_25), "right", false);
        procHolder.put(qn, QueryLanguage.ALL, "both", false);
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_5)).isEqualTo("both");
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_25)).isEqualTo("both");
    }

    @Test
    void canUpdateToSeparateScopes() {
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qn = new QualifiedName("qn");
        procHolder.put(qn, QueryLanguage.ALL, "both", false);
        procHolder.put(qn, Set.of(QueryLanguage.CYPHER_5), "left", false);
        procHolder.put(qn, Set.of(QueryLanguage.CYPHER_25), "right", false);
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_5)).isEqualTo("left");
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_25)).isEqualTo("right");
    }

    @Test
    void shouldBeAbleToQueryPerScope() {
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        String value = "Hello";
        QualifiedName qn = new QualifiedName("qn");
        procHolder.put(qn, QueryLanguage.ALL, value, false);
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_5)).isEqualTo(value);
        assertThat(procHolder.getByKey(qn, QueryLanguage.CYPHER_25)).isEqualTo(value);
    }

    @ParameterizedTest
    @MethodSource
    void hasDifferentScopes(int[] entry, Set<QueryLanguage> scopes, boolean expected) {
        assertThat(ProcedureHolder.hasDifferentScopes(entry, scopes)).isEqualTo(expected);
    }

    static Stream<Arguments> hasDifferentScopes() {
        var unused = ProcedureHolder.UNUSED_REFERENCE;
        Set<QueryLanguage> none = Set.of();
        var only_5 = Set.of(QueryLanguage.CYPHER_5);
        var only_25 = Set.of(QueryLanguage.CYPHER_25);
        var all = QueryLanguage.ALL;
        return Stream.of(
                Arguments.of(new int[] {unused, 81}, none, true),
                Arguments.of(new int[] {unused, 81}, only_5, false),
                Arguments.of(new int[] {unused, 81}, only_25, true),
                Arguments.of(new int[] {unused, 81}, all, true),
                Arguments.of(new int[] {81, unused}, none, true),
                Arguments.of(new int[] {81, unused}, only_5, true),
                Arguments.of(new int[] {81, unused}, only_25, false),
                Arguments.of(new int[] {81, unused}, all, true),
                Arguments.of(new int[] {unused, unused}, none, false),
                Arguments.of(new int[] {unused, unused}, only_5, true),
                Arguments.of(new int[] {unused, unused}, only_25, true),
                Arguments.of(new int[] {unused, unused}, all, true),
                Arguments.of(new int[] {81, 91}, none, true),
                Arguments.of(new int[] {81, 91}, only_5, true),
                Arguments.of(new int[] {81, 91}, only_25, true),
                Arguments.of(new int[] {81, 91}, all, false));
    }
}
