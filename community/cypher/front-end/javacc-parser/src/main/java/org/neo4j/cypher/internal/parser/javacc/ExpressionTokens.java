/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.parser.javacc;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

public class ExpressionTokens
{
    private static final Set<Integer> expressions = new HashSet<>( Arrays.asList(
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
            UNSIGNED_OCTAL_INTEGER
    ) );

    public static Set<Integer> getExpressionTokens()
    {
        return expressions;
    }
}
