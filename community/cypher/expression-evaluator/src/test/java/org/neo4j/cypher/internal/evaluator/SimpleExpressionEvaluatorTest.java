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
package org.neo4j.cypher.internal.evaluator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.cypher.internal.evaluator.Evaluator.expressionEvaluator;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.cypher.internal.CypherVersion;

class SimpleExpressionEvaluatorTest {

    private final ExpressionEvaluator evaluator = expressionEvaluator(CypherVersion.Default);

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldConvertToSpecificType(CypherVersion version) throws EvaluationException {
        assertThat(evaluate(version, "[1, 2, 3]", List.class)).isEqualTo(List.of(1L, 2L, 3L));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldConvertToObject(CypherVersion version) throws EvaluationException {
        assertThat(evaluate(version, "{prop: 42}", Object.class)).isEqualTo(Map.of("prop", 42L));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldThrowIfWrongType(CypherVersion version) {
        // Expect
        assertThatThrownBy(() -> evaluator.evaluate("{prop: 42}", List.class))
                .isExactlyInstanceOf(EvaluationException.class);
    }

    private Object evaluate(CypherVersion version, String expression, Class<?> cls) throws EvaluationException {
        return expressionEvaluator(version).evaluate(expression, cls);
    }
}
