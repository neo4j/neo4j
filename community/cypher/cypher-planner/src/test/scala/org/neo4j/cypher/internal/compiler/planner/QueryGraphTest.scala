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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryGraph.PredicatesByDependencies
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class QueryGraphTest extends CypherFunSuite with AstConstructionTestSupport {
  val x = "x"
  val n = "n"
  val m = "m"
  val c = "c"
  val r1 = "r1"
  val r2 = "r2"
  val r3 = "r3"

  private val hint1 =
    UsingIndexHint(varFor("n"), LabelOrRelTypeName("Label")(pos), Seq(PropertyKeyName("prop1")(pos)))(pos)

  private val hint2 =
    UsingIndexHint(varFor("m"), LabelOrRelTypeName("Label")(pos), Seq(PropertyKeyName("prop2")(pos)))(pos)

  test("addHints should add new hints") {
    val qg1 = QueryGraph(hints = Set(hint1))
    val qg2 = QueryGraph(hints = Set(hint1, hint2))

    qg1.addHints(Set(hint2)) should equal(qg2)
  }

  test("addHint should not add already existing hint") {
    val qg1 = QueryGraph(hints = Set(hint1))
    val qg2 = QueryGraph(hints = Set(hint1, hint2))

    qg1.addHints(Set(hint1, hint2)) should equal(qg2)
  }

  test("withoutHints should remove hints") {
    val qg1 = QueryGraph(hints = Set(hint1, hint2))
    val qg2 = QueryGraph(hints = Set(hint1))

    qg1.removeHints(Set(hint2)) should equal(qg2)
  }

  test("should not get duplicate hints when combining query graphs") {
    val hint3 =
      UsingIndexHint(varFor("o"), LabelOrRelTypeName("Label")(pos), Seq(PropertyKeyName("prop3")(pos)))(pos)
    val qg1 = QueryGraph(hints = Set(hint1, hint2))
    val qg2 = QueryGraph(hints = Set(hint1, hint3))
    val qg3 = QueryGraph(hints = Set(hint1, hint2, hint3))

    qg1 ++ qg2 should equal(qg3)
  }

  test("should not mutate QueryGraph.empty state") {
    val qg = QueryGraph.empty

    qg.allQGsWithLeafInfo.foreach(_.allKnownUnstableNodeLabels(SemanticTable()))
    qg.allQGsWithLeafInfo.foreach(_.allKnownUnstableNodeLabels.cacheSize shouldBe 1)

    QueryGraph.empty.allQGsWithLeafInfo.foreach(_.allKnownUnstableNodeLabels.cacheSize shouldBe 0)
  }

  test("should partition predicates by dependency on non-argument ids") {

    def pred(deps: LogicalVariable*): Predicate = Predicate(deps.toSet, trueLiteral)

    val predicates = Set(
      pred(),
      pred(v"arg1"),
      pred(v"arg2"),
      pred(v"arg1", v"arg3"),
      pred(v"arg3", v"x"),
      pred(v"y"),
      pred(v"x", v"z"),
      pred(v"arg1", v"arg2", v"arg3", v"x", v"y", v"z")
    )

    val qg = QueryGraph.empty
      .addArgumentIds(Seq(v"arg1", v"arg2", v"arg3"))
      .addPredicates(predicates)

    qg.predicatesPartitionedByDependencyOnNonArgumentIds shouldEqual
      PredicatesByDependencies(
        dependOnArgumentsOnly = Set(
          pred(),
          pred(v"arg1"),
          pred(v"arg2"),
          pred(v"arg1", v"arg3")
        ),
        hasNonArgumentDependencies = Set(
          pred(v"arg3", v"x"),
          pred(v"y"),
          pred(v"x", v"z"),
          pred(v"arg1", v"arg2", v"arg3", v"x", v"y", v"z")
        )
      )
  }
}
