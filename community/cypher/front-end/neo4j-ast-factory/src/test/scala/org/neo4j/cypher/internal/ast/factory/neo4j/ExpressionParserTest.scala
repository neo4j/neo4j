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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING

class ExpressionParserTest extends JavaccParserAstTestBase[Expression] {

  implicit private val parser: JavaccRule[Expression] = JavaccRule.Expression

  test("thing CONTAINS 'a' + 'b'") {
    gives(contains(varFor("thing"), add(literalString("a"), literalString("b"))))
  }

  test("thing STARTS WITH 'a' + 'b'") {
    gives(startsWith(varFor("thing"), add(literalString("a"), literalString("b"))))
  }

  test("thing ENDS WITH 'a' + 'b'") {
    gives(endsWith(varFor("thing"), add(literalString("a"), literalString("b"))))
  }

  test("2*(2.0-1.5)") {
    gives {
      multiply(literal(2), subtract(literal(2.0), literal(1.5)))
    }
  }

  test("+1.5") {
    gives {
      unaryAdd(literal(1.5))
    }
  }

  test("+1") {
    gives {
      unaryAdd(literal(1))
    }
  }

  test("2*(2.0 - +1.5)") {
    gives {
      multiply(literal(2), subtract(literal(2.0), unaryAdd(literal(1.5))))
    }
  }

  test("0-1") {
    gives {
      subtract(literal(0), literal(1))
    }
  }

  test("0-0.1") {
    gives {
      subtract(literal(0), literal(0.1))
    }
  }

  test("[p = (n)-->() where last(nodes(p)):End | p]") {
    gives {
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
      )(pos, Set.empty, "", "")
    }
  }
}
