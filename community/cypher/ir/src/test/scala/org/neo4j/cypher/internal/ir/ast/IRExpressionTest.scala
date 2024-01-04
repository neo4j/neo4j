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
package org.neo4j.cypher.internal.ir.ast

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.topDown

class IRExpressionTest extends CypherFunSuite with AstConstructionTestSupport {

  test("ListIRExpression should return the correct dependencies") {
    val e = ListIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = Set(v"a"),
          patternNodes = Set(v"a", v"b"),
          // This is to make sure that the appearance of varFor("b") does not add b as a dependency
          selections = Selections.from(varFor("b"))
        )
      ),
      varFor("anon_0"),
      varFor("anon_1"),
      "ListIRExpression"
    )(pos, Some(Set(varFor("b"))), Some(Set(varFor("a"))))

    e.dependencies should equal(Set(varFor("a")))
  }

  test("ExistsIRExpression should return the correct dependencies") {
    val e = ExistsIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = Set(v"a"),
          patternNodes = Set(v"a", v"b"),
          // This is to make sure that the appearance of varFor("b") does not add b as a dependency
          selections = Selections.from(varFor("b"))
        )
      ),
      varFor("anon_0"),
      "ExistsIRExpression"
    )(pos, Some(Set(varFor("b"))), Some(Set(varFor("a"))))

    e.dependencies should equal(Set(varFor("a")))
  }

  test("ListIRExpression contained in another expression should return the correct dependencies") {
    val e = listOf(ListIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = Set(v"a"),
          patternNodes = Set(v"a", v"b"),
          // This is to make sure that the appearance of varFor("b") does not add b as a dependency
          selections = Selections.from(varFor("b"))
        )
      ),
      varFor("anon_0"),
      varFor("anon_1"),
      "ListIRExpression"
    )(pos, Some(Set(varFor("b"))), Some(Set(varFor("a")))))

    e.dependencies should equal(Set(varFor("a")))
  }

  test("Can rewrite node in IR expression") {
    val e = ListIRExpression(
      RegularSinglePlannerQuery(
        QueryGraph(
          argumentIds = Set(v"a"),
          patternNodes = Set(v"a", v"b")
        )
      ),
      varFor("anon_0"),
      varFor("anon_1"),
      "ListIRExpression"
    )(pos, Some(Set(v"b")), Some(Set(v"a")))

    def rename(v: LogicalVariable): LogicalVariable = v match {
      case v @ LogicalVariable("a") => Variable("o")(v.position)
      case _                        => v
    }

    val rewriter = topDown(Rewriter.lift {
      case lv: LogicalVariable => rename(lv)
      case e: ExpressionWithComputedDependencies =>
        val newIntroducedVariables = e.introducedVariables.map(rename)
        val newScopeDependencies = e.scopeDependencies.map(rename)
        e.withComputedIntroducedVariables(newIntroducedVariables).withComputedScopeDependencies(newScopeDependencies)
    })

    val rewritten = e.endoRewrite(rewriter)

    rewritten should equal(
      ListIRExpression(
        RegularSinglePlannerQuery(
          QueryGraph(
            argumentIds = Set(v"o"),
            patternNodes = Set(v"o", v"b")
          )
        ),
        varFor("anon_0"),
        varFor("anon_1"),
        "ListIRExpression"
      )(pos, Some(Set(v"b")), Some(Set(v"o")))
    )
  }
}
