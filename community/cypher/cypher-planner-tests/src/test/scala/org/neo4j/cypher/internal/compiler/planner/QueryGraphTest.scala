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
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryGraph.DependentElements
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.VectorSearchClause
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

class QueryGraphTest extends CypherPlannerTestSuite with AstConstructionTestSupport {
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
    val qg1 = QueryGraph(hints = ListSet(hint1))
    val qg2 = QueryGraph(hints = ListSet(hint1, hint2))

    qg1.addHints(ListSet(hint2)) should equal(qg2)
  }

  test("addHint should not add already existing hint") {
    val qg1 = QueryGraph(hints = ListSet(hint1))
    val qg2 = QueryGraph(hints = ListSet(hint1, hint2))

    qg1.addHints(ListSet(hint1, hint2)) should equal(qg2)
  }

  test("withoutHints should remove hints") {
    val qg1 = QueryGraph(hints = ListSet(hint1, hint2))
    val qg2 = QueryGraph(hints = ListSet(hint1))

    qg1.removeHints(ListSet(hint2)) should equal(qg2)
  }

  test("should not get duplicate hints when combining query graphs") {
    val hint3 =
      UsingIndexHint(varFor("o"), LabelOrRelTypeName("Label")(pos), Seq(PropertyKeyName("prop3")(pos)))(pos)
    val qg1 = QueryGraph(hints = ListSet(hint1, hint2))
    val qg2 = QueryGraph(hints = ListSet(hint1, hint3))
    val qg3 = QueryGraph(hints = ListSet(hint1, hint2, hint3))

    qg1 ++ qg2 should equal(qg3)
  }

  test("should partition predicates by dependency on non-argument ids") {

    def pred(deps: LogicalVariable*): Predicate = Predicate(deps.toSet, trueLiteral)

    def sp(start: LogicalVariable, end: LogicalVariable): ShortestRelationshipPattern = {
      ShortestRelationshipPattern(
        None,
        PatternRelationship(
          varFor(start.name + end.name),
          (start, end),
          SemanticDirection.OUTGOING,
          Seq.empty,
          VarPatternLength(1, Some(5))
        ),
        true
      )(null)
    }

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

    val shortestRels = Set(
      sp(v"x", v"y"),
      sp(v"x", v"arg1"),
      sp(v"arg2", v"y"),
      sp(v"arg1", v"arg2"),
      sp(v"arg2", v"arg3")
    )

    val qg = QueryGraph.empty
      .addArgumentIds(Seq(v"arg1", v"arg2", v"arg3"))
      .addPredicates(predicates)
      .addShortestRelationships(shortestRels)

    qg.dependentElementsPartitionedByDependencyOnNonArgumentIds shouldEqual
      DependentElements(
        dependOnArgumentsOnly = DependentElements.Bucket(
          predicates = Set(
            pred(),
            pred(v"arg1"),
            pred(v"arg2"),
            pred(v"arg1", v"arg3")
          ),
          shortestRelationshipPatterns = Set(
            sp(v"arg1", v"arg2"),
            sp(v"arg2", v"arg3")
          ),
          searchClause = None
        ),
        hasNonArgumentDependencies = DependentElements.Bucket(
          predicates = Set(
            pred(v"arg3", v"x"),
            pred(v"y"),
            pred(v"x", v"z"),
            pred(v"arg1", v"arg2", v"arg3", v"x", v"y", v"z")
          ),
          shortestRelationshipPatterns = Set(
            sp(v"x", v"y"),
            sp(v"x", v"arg1"),
            sp(v"arg2", v"y")
          ),
          searchClause = None
        )
      )
  }

  test("should classify SEARCH as having non-argument dependencies") {
    val baseSearchClause = VectorSearchClause(
      resultVariable = v"movie",
      indexName = "moviePlots",
      embedding = listOfInt(1, 2, 3),
      where = None,
      limit = literal(10),
      scoreVariable = None
    )

    val arg = v"arg"

    val hasNonArgumentDependenciesTestCases: Seq[VectorSearchClause] = Seq(
      // no dependencies of any kind
      baseSearchClause,
      // embedding depends on non-argument symbol
      baseSearchClause.copy(embedding = prop("n", "embedding")),
      // embedding depends on argument, but not the result variable
      baseSearchClause.copy(embedding = prop(arg.name, "embedding")),
      // result variable is an argument, embedding depends on non-argument symbol
      baseSearchClause.copy(resultVariable = arg, embedding = prop("n", "embedding"))
    )

    for (search <- hasNonArgumentDependenciesTestCases) withClue(search) {
      val qg = QueryGraph.empty
        .addArgumentId(arg)
        .addSearchClause(Some(search))

      val dependentElements = qg.dependentElementsPartitionedByDependencyOnNonArgumentIds
      dependentElements.dependOnArgumentsOnly.searchClause shouldEqual None
      dependentElements.hasNonArgumentDependencies.searchClause shouldEqual Some(search)
    }
  }

  test("should classify SEARCH as having argument-only dependencies") {
    val baseSearchClause = VectorSearchClause(
      resultVariable = v"movie",
      indexName = "moviePlots",
      embedding = listOfInt(1, 2, 3),
      where = None,
      limit = literal(10),
      scoreVariable = None
    )

    val arg = v"arg"

    val hasArgumentOnlyDependenciesTestCases: Seq[VectorSearchClause] = Seq(
      // result variable is an argument, no dependencies elsewhere
      baseSearchClause.copy(resultVariable = arg),
      // result variable is an argument, argument dependencies in embedding
      baseSearchClause.copy(resultVariable = arg, embedding = prop(arg.name, "embedding"))
    )

    for (search <- hasArgumentOnlyDependenciesTestCases) withClue(search) {
      val qg = QueryGraph.empty
        .addArgumentId(arg)
        .addSearchClause(Some(search))

      val dependentElements = qg.dependentElementsPartitionedByDependencyOnNonArgumentIds
      dependentElements.dependOnArgumentsOnly.searchClause shouldEqual Some(search)
      dependentElements.hasNonArgumentDependencies.searchClause shouldEqual None
    }
  }

}
