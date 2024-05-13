/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.parser.javacc;

import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParseExceptionsTest {
    @Test
    void removeSingleIdentifierToken() {
        int[][] expectedTokenSequence = {{CypherConstants.IDENTIFIER, CypherConstants.ACCESS}};
        Assertions.assertEquals(
                ParseExceptions.expected(expectedTokenSequence, CypherConstants.tokenImage),
                Arrays.asList("an identifier"));
    }

    @Test
    void removeSingleIdentifierTokenWithParameter() {
        int[][] expectedTokenSequence = {{CypherConstants.IDENTIFIER, CypherConstants.ACCESS, CypherConstants.DOLLAR}};
        Assertions.assertEquals(
                ParseExceptions.expected(expectedTokenSequence, CypherConstants.tokenImage),
                Arrays.asList("a parameter", "an identifier"));
    }

    @Test
    void keepDoubleIdentifierToken() {
        int[][] expectedTokenSequence = {{CypherConstants.IDENTIFIER, CypherConstants.ACCESS}, {CypherConstants.ACCESS}
        };
        Assertions.assertEquals(
                ParseExceptions.expected(expectedTokenSequence, CypherConstants.tokenImage),
                Arrays.asList("\"ACCESS\"", "an identifier"));
    }

    @Test
    void removeSingleIdentifierTokenInExpression() {
        int[][] expectedTokenSequence = {
            {CypherConstants.IDENTIFIER, CypherConstants.PLUS},
            {CypherConstants.ACCESS, CypherConstants.UNSIGNED_DECIMAL_INTEGER}
        };
        Assertions.assertEquals(
                ParseExceptions.expected(expectedTokenSequence, CypherConstants.tokenImage),
                Arrays.asList("an expression"));
    }

    @Test
    void removeReduceToDoubleIdentifierTokenInExpression() {
        int[][] expectedTokenSequence = {
            {CypherConstants.IDENTIFIER, CypherConstants.PLUS, CypherConstants.DOLLAR},
            {CypherConstants.PLUS, CypherConstants.UNSIGNED_DECIMAL_INTEGER}
        };
        Assertions.assertEquals(
                ParseExceptions.expected(expectedTokenSequence, CypherConstants.tokenImage),
                Arrays.asList("\"+\"", "an expression"));
    }

    @Test
    void twoExpressions() {
        int[][] expectedTokenSequence = {
            {CypherConstants.IDENTIFIER, CypherConstants.PLUS}, {CypherConstants.IDENTIFIER, CypherConstants.PLUS}
        };
        Assertions.assertEquals(
                ParseExceptions.expected(expectedTokenSequence, CypherConstants.tokenImage),
                Arrays.asList("an expression"));
    }
}
