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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class LogicalPlanTreeRendererTest extends CypherFunSuite {

  implicit val idGen: SameId = SameId(Id(0))
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  test("Should render plan with depth 0") {
    val plan = ProduceResult(
      Limit(
        AllNodesScan(varFor("n"), Set.empty),
        SignedDecimalIntegerLiteral("10")(InputPosition.NONE)
      ),
      Seq.empty
    )

    render(plan) shouldEqual
      """ProduceResult
        #Limit
        #AllNodesScan""".stripMargin('#')
  }

  test("Should render plan with depth 1") {
    val plan = ProduceResult(
      CartesianProduct(
        AllNodesScan(varFor("n"), Set.empty),
        AllNodesScan(varFor("m"), Set.empty)
      ),
      Seq.empty
    )

    render(plan) shouldEqual
      """ProduceResult
        #CartesianProduct
        #| AllNodesScan
        #AllNodesScan""".stripMargin('#')
  }

  test("Should render complex plan with depth >1") {
    val cp = CartesianProduct(
      AllNodesScan(varFor("n"), Set.empty),
      AllNodesScan(varFor("m"), Set.empty)
    )

    val innerApply = Apply(
      cp,
      cp
    )

    val plan = ProduceResult(
      Apply(
        Limit(
          Apply(
            NodeByLabelScan(varFor("x"), LabelName("X")(InputPosition.NONE), Set.empty, IndexOrderNone),
            innerApply
          ),
          SignedDecimalIntegerLiteral("10")(InputPosition.NONE)
        ),
        cp
      ),
      returnColumns = Seq(Column(varFor("n"), Set.empty))
    )

    render(plan) shouldEqual
      """ProduceResult
        #Apply
        #| CartesianProduct
        #| | AllNodesScan
        #| AllNodesScan
        #Limit
        #Apply
        #| Apply
        #| | CartesianProduct
        #| | | AllNodesScan
        #| | AllNodesScan
        #| CartesianProduct
        #| | AllNodesScan
        #| AllNodesScan
        #NodeByLabelScan""".stripMargin('#')
  }

  def render(plan: LogicalPlan): String = {
    LogicalPlanTreeRenderer.render(plan, "| ", _.productPrefix)
  }
}
