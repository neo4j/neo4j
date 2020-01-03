/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.evaluator;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.helpers.collection.MapUtil;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SimpleExpressionEvaluatorTest
{
    @Test
    void shouldConvertToSpecificType() throws EvaluationException
    {
        // Given
        ExpressionEvaluator evaluator = Evaluator.expressionEvaluator();

        // When
        List<?> list = evaluator.evaluate( "[1, 2, 3]", List.class );

        // Then
        assertEquals( asList( 1L, 2L, 3L ), list );
    }

    @Test
    void shouldConvertToObject() throws EvaluationException
    {
        // Given
        ExpressionEvaluator evaluator = Evaluator.expressionEvaluator();

        // When
        Object object = evaluator.evaluate( "{prop: 42}", Object.class );

        // Then
        assertEquals( MapUtil.map( "prop", 42L ), object );
    }

    @Test
    void shouldThrowIfWrongType()
    {
        // Given
        ExpressionEvaluator evaluator = Evaluator.expressionEvaluator();

        // Expect
        assertThrows( EvaluationException.class, () -> evaluator.evaluate( "{prop: 42}", List.class ) );
    }
}
