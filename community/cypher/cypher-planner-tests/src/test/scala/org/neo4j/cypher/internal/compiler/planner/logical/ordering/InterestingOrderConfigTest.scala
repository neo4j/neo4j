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
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrderCandidate
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate

class InterestingOrderConfigTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport {

  test("should report required order on variable") {
    val q = buildSinglePlannerQuery(
      """MATCH (a)
        |RETURN *
        |  ORDER BY a""".stripMargin
    )
    InterestingOrderConfig.interestingOrderForPart(q, false) shouldBe
      InterestingOrderConfig(
        InterestingOrder.required(
          RequiredOrderCandidate(Seq(ColumnOrder.Asc(v"a", Map(v"a" -> v"a"))))
        )
      )
  }

  test("should find required order beyond mutation") {
    val q = buildSinglePlannerQuery(
      """MATCH (a:A)
        |  WHERE a.prop IS NOT NULL AND a.foo IS NOT NULL
        |CREATE (newNode)
        |RETURN a
        |  ORDER BY a.prop""".stripMargin
    )
    InterestingOrderConfig.interestingOrderForPart(q, false) shouldBe
      InterestingOrderConfig(
        InterestingOrder.required(
          RequiredOrderCandidate(Seq(ColumnOrder.Asc(prop("a", "prop"), Map(v"a" -> v"a"))))
        )
      )
  }

  test("should find required order beyond mutation in horizon") {
    val q = buildSinglePlannerQuery(
      """MATCH (a)
        |CALL (a) {
        |  SET a.prop = 10
        |}
        |RETURN a
        |  ORDER BY a.prop""".stripMargin
    )
    val order = Seq(ColumnOrder.Asc(prop("a", "prop"), Map(v"a" -> v"a")))
    InterestingOrderConfig.interestingOrderForPart(q, false) shouldBe
      InterestingOrderConfig(
        InterestingOrder.interested(
          InterestingOrderCandidate(order)
        ),
        InterestingOrder(
          RequiredOrderCandidate(order),
          Seq(InterestingOrderCandidate(order))
        )
      )
  }

  test("should find required order before mutation in horizon") {
    val q = buildSinglePlannerQuery(
      """MATCH (a)
        |WITH a
        |  ORDER BY a.prop
        |CALL (a) {
        |  SET a.prop = 10
        |}""".stripMargin
    )
    InterestingOrderConfig.interestingOrderForPart(q, false) shouldBe
      InterestingOrderConfig(
        InterestingOrder.required(
          RequiredOrderCandidate(Seq(ColumnOrder.Asc(prop("a", "prop"), Map(v"a" -> v"a"))))
        )
      )
  }

  test("should report required order in horizon") {
    val query = buildSinglePlannerQuery(
      """MATCH (a:A)
        |  WHERE a.prop > 42
        |RETURN a
        |  ORDER BY a.prop""".stripMargin
    )

    InterestingOrderConfig.interestingOrderForPart(query, false) shouldBe
      InterestingOrderConfig(
        InterestingOrder.required(
          RequiredOrderCandidate(Seq(ColumnOrder.Asc(prop("a", "prop"), Map(v"a" -> v"a"))))
        )
      )
  }

  test("should report interesting order in horizon") {
    val query = buildSinglePlannerQuery(
      """MATCH (a)
        |RETURN DISTINCT a.prop""".stripMargin
    )

    InterestingOrderConfig.interestingOrderForPart(query, false) shouldBe
      InterestingOrderConfig(InterestingOrder(
        RequiredOrderCandidate.empty,
        Seq(
          InterestingOrderCandidate(Seq(ColumnOrder.Asc(prop("a", "prop")))),
          InterestingOrderCandidate(Seq(ColumnOrder.Desc(prop("a", "prop"))))
        )
      ))
  }
}
