/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.PatternExpressionPatternElementNamer
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._

class PatternExpressionPatternElementNamerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should not touch anything if everything is named") {
    val original = parsePatternExpression("WITH {a} AS a, {r} AS r, {b} AS b LIMIT 1 RETURN (a)-[r]->(b)")
    val (actual, map) = PatternExpressionPatternElementNamer(original)
    val expected = parsePatternExpression("WITH {a} AS a, {r} AS r, {b} AS b LIMIT 1 RETURN (a)-[r]->(b)")

    actual should equal(expected)
    map should be(empty)
  }

  test("should name nodes") {
    val original = parsePatternExpression("WITH {a} AS a, {r} AS r, {b} AS b LIMIT 1 RETURN ()-[r]->(b)")
    val (actual, map) = PatternExpressionPatternElementNamer(original)
    val expected = parsePatternExpression("WITH {a} AS a, {r} AS r, {b} AS b LIMIT 1 RETURN (`  NODE50`)-[r]->(b)")

    actual should equal(expected)
    processMap(map) should equal(Map(49 -> "  NODE50"))
  }

  test("should name relationships") {
    val original = parsePatternExpression("WITH {a} AS a, {r} AS r, {b} AS b LIMIT 1 RETURN (a)-[]->(b)")
    val (actual, map) = PatternExpressionPatternElementNamer(original)
    val expected = parsePatternExpression("WITH {a} AS a, {r} AS r, {b} AS b LIMIT 1 RETURN (a)-[`  REL53`]->(b)")

    actual should equal(expected)
    processMap(map) should equal(Map(52 -> "  REL53"))
  }

  test("should rename multiple nodes") {
    val original = parsePatternExpression("WITH {r} AS r LIMIT 1 RETURN ()-[r]->()")
    val (actual, map) = PatternExpressionPatternElementNamer(original)
    val expected = parsePatternExpression("WITH {r} AS r LIMIT 1 RETURN (`  NODE30`)-[r]->(`  NODE38`)")

    actual should equal(expected)
    processMap(map) should equal(Map(29 -> "  NODE30", 37 -> "  NODE38"))
  }

  test("should rename multiple relationships") {
    val original = parsePatternExpression("WITH {a} AS a, {b} AS b, {c} AS c LIMIT 1 RETURN (a)-[]-(b)-[]-(c)")
    val (actual, map) = PatternExpressionPatternElementNamer(original)
    val expected = parsePatternExpression("WITH {a} AS a, {b} AS b, {c} AS c LIMIT 1 RETURN (a)-[`  REL53`]-(b)-[`  REL60`]-(c)")

    actual should equal(expected)
    processMap(map) should equal(Map(52 -> "  REL53", 59 -> "  REL60"))
  }

  private def processMap(map: Map[PatternElement, Variable]) = {
    map.collect {
      case (pattern: NodePattern, ident) => pattern.position.offset -> ident.name
      case (pattern: RelationshipChain, ident) => pattern.relationship.position.offset -> ident.name
    }.toMap
  }

  private def parsePatternExpression(query: String): PatternExpression = {
    parser.parse(query) match {
      case Query(_, SingleQuery(clauses)) =>
        val ret = clauses.last.asInstanceOf[Return]
        val patExpr = ret.returnItems.items.head.expression.asInstanceOf[PatternExpression]
        patExpr
    }
  }
}
