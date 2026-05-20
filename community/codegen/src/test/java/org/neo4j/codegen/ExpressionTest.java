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
package org.neo4j.codegen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.neo4j.codegen.Expression.FALSE;
import static org.neo4j.codegen.Expression.NULL;
import static org.neo4j.codegen.Expression.TRUE;
import static org.neo4j.codegen.Expression.and;
import static org.neo4j.codegen.Expression.equal;
import static org.neo4j.codegen.Expression.gt;
import static org.neo4j.codegen.Expression.gte;
import static org.neo4j.codegen.Expression.invoke;
import static org.neo4j.codegen.Expression.lt;
import static org.neo4j.codegen.Expression.lte;
import static org.neo4j.codegen.Expression.not;
import static org.neo4j.codegen.Expression.notEqual;
import static org.neo4j.codegen.Expression.or;
import static org.neo4j.codegen.MethodReference.methodReference;

import org.junit.jupiter.api.Test;

public class ExpressionTest {
    @Test
    void shouldNegateTrueToFalse() {
        assertThat(not(TRUE)).isSameAs(FALSE);
        assertThat(not(FALSE)).isSameAs(TRUE);
    }

    @Test
    void shouldRemoveDoubleNegation() {
        Expression expression = invoke(methodReference(getClass(), boolean.class, "TRUE"));
        assertThat(not(not(expression))).isSameAs(expression);
    }

    @Test
    void shouldOptimizeNullChecks() {
        // given
        ExpressionVisitor visitor = mock(ExpressionVisitor.class);
        Expression expression = invoke(methodReference(getClass(), Object.class, "value"));

        // when
        equal(expression, NULL).accept(visitor);

        // then
        verify(visitor).isNull(expression);

        reset(visitor); // next

        // when
        equal(NULL, expression).accept(visitor);

        // then
        verify(visitor).isNull(expression);

        reset(visitor); // next

        // when
        not(equal(expression, NULL)).accept(visitor);

        // then
        verify(visitor).notNull(expression);

        reset(visitor); // next

        // when
        not(equal(NULL, expression)).accept(visitor);

        // then
        verify(visitor).notNull(expression);
    }

    @Test
    void shouldOptimizeNegatedInequalities() {
        // given
        ExpressionVisitor visitor = mock(ExpressionVisitor.class);
        Expression expression = invoke(methodReference(getClass(), Object.class, "value"));

        // when
        not(gt(expression, expression)).accept(visitor);

        // then
        verify(visitor).lte(expression, expression);

        reset(visitor); // next

        // when
        not(gte(expression, expression)).accept(visitor);

        // then
        verify(visitor).lt(expression, expression);

        reset(visitor); // next

        // when
        not(lt(expression, expression)).accept(visitor);

        // then
        verify(visitor).gte(expression, expression);

        reset(visitor); // next

        // when
        not(lte(expression, expression)).accept(visitor);

        // then
        verify(visitor).gt(expression, expression);

        reset(visitor); // next

        // when
        not(equal(expression, expression)).accept(visitor);

        // then
        verify(visitor).notEqual(expression, expression);

        reset(visitor); // next

        // when
        not(notEqual(expression, expression)).accept(visitor);

        // then
        verify(visitor).equal(expression, expression);
    }

    @Test
    void shouldOptimizeBooleanCombinationsWithConstants() {
        // given
        Expression expression = invoke(methodReference(getClass(), boolean.class, "TRUE"));

        // then
        assertThat(and(expression, TRUE)).isSameAs(expression);
        assertThat(and(TRUE, expression)).isSameAs(expression);
        assertThat(and(expression, FALSE)).isSameAs(FALSE);
        assertThat(and(FALSE, expression)).isSameAs(FALSE);

        assertThat(or(expression, FALSE)).isSameAs(expression);
        assertThat(or(FALSE, expression)).isSameAs(expression);
        assertThat(or(expression, TRUE)).isSameAs(TRUE);
        assertThat(or(TRUE, expression)).isSameAs(TRUE);
    }

    public static boolean TRUE() {
        return true;
    }

    public static Object value() {
        return null;
    }
}
