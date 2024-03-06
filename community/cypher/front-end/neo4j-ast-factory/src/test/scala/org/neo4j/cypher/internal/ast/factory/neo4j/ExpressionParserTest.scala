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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate

class ExpressionParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("[true IN [1, 2]]") {
    parsesTo[Expression](listComprehension(varFor("true"), listOf(literalInt(1), literalInt(2)), None, None))
  }

  test("[(true IN [1, 2])]") {
    parsesTo[Expression](listOf(in(trueLiteral, listOf(literalInt(1), literalInt(2)))))
  }

  test("[create IN [1, 2]]") {
    parsesTo[Expression](listComprehension(varFor("create"), listOf(literalInt(1), literalInt(2)), None, None))
  }

  test("[not IN [1, 2]]") {
    parsesTo[Expression](listComprehension(varFor("not"), listOf(literalInt(1), literalInt(2)), None, None))
  }

  test("[starts IN [1, 2]]") {
    parsesTo[Expression](listComprehension(varFor("starts"), listOf(literalInt(1), literalInt(2)), None, None))
  }

  test("[true IN [ true, false ], false]") {
    parsesTo[Expression](listOf(in(trueLiteral, listOf(trueLiteral, falseLiteral)), falseLiteral))
  }

  test("thing CONTAINS 'a' + 'b'") {
    parsesTo[Expression](contains(varFor("thing"), add(literalString("a"), literalString("b"))))
  }

  test("thing STARTS WITH 'a' + 'b'") {
    parsesTo[Expression](startsWith(varFor("thing"), add(literalString("a"), literalString("b"))))
  }

  test("thing ENDS WITH 'a' + 'b'") {
    parsesTo[Expression](endsWith(varFor("thing"), add(literalString("a"), literalString("b"))))
  }

  test("2*(2.0-1.5)") {
    parsesTo[Expression] {
      multiply(literal(2), subtract(literal(2.0), literal(1.5)))
    }
  }

  test("+1.5") {
    parsesTo[Expression] {
      unaryAdd(literal(1.5))
    }
  }

  test("+1") {
    parsesTo[Expression] {
      unaryAdd(literal(1))
    }
  }

  test("2*(2.0 - +1.5)") {
    parsesTo[Expression] {
      multiply(literal(2), subtract(literal(2.0), unaryAdd(literal(1.5))))
    }
  }

  test("0-1") {
    parsesTo[Expression] {
      subtract(literal(0), literal(1))
    }
  }

  test("0-0.1") {
    parsesTo[Expression] {
      subtract(literal(0), literal(0.1))
    }
  }

  test("[p = (n)-->() where last(nodes(p)):End | p]") {
    parsesTo[Expression] {
      PatternComprehension(
        namedPath = Some(varFor("p")),
        pattern = RelationshipsPattern(RelationshipChain(
          NodePattern(Some(varFor("n")), None, None, None)(pos),
          RelationshipPattern(None, None, None, None, None, OUTGOING)(pos),
          NodePattern(None, None, None, None)(pos)
        )(pos))(pos),
        predicate = Some(LabelExpressionPredicate(
          function("last", function("nodes", varFor("p"))),
          labelOrRelTypeLeaf("End")
        )(pos)),
        projection = varFor("p")
      )(pos, None, None)
    }
  }
}
