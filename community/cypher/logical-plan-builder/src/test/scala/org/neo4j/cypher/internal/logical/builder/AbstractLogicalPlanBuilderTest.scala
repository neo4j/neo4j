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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AbstractLogicalPlanBuilderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should allocate sequential logical plan ids") {
    // when
    val plan = new TestPlanBuilder()
      .produceResults("x")
      .filter("x = 1")
      .filter("x >= 1")
      .filter("x < 2")
      .filter("x != 3")
      .input(variables = List("x"))
      .build()

    // then
    var nextOperator: Option[LogicalPlan] = Some(plan)
    var id = 0
    while (nextOperator.isDefined) {
      val operator = nextOperator.get
      operator.id shouldBe Id(id)
      id += 1
      nextOperator = operator.lhs
    }
  }
}
