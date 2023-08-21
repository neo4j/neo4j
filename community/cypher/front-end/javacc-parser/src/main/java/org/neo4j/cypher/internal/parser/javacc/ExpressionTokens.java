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

import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DECIMAL_DOUBLE;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.DOLLAR;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.IDENTIFIER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LBRACKET;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LCURLY;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.LPAREN;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.MINUS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.PLUS;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STRING_LITERAL1;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.STRING_LITERAL2;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNSIGNED_DECIMAL_INTEGER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNSIGNED_HEX_INTEGER;
import static org.neo4j.cypher.internal.parser.javacc.CypherConstants.UNSIGNED_OCTAL_INTEGER;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ExpressionTokens {
    private static final Set<Integer> expressions = new HashSet<>(Arrays.asList(
            DECIMAL_DOUBLE,
            DOLLAR,
            IDENTIFIER,
            LBRACKET,
            LCURLY,
            LPAREN,
            MINUS,
            PLUS,
            STRING_LITERAL1,
            STRING_LITERAL2,
            UNSIGNED_DECIMAL_INTEGER,
            UNSIGNED_HEX_INTEGER,
            UNSIGNED_OCTAL_INTEGER));

    public static Set<Integer> getExpressionTokens() {
        return expressions;
    }
}
