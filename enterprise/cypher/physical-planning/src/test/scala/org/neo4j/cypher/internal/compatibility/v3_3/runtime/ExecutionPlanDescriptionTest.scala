/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ast.ReferenceFromSlot
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments.{DbHits, EstimatedRows, Expression, Rows}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.{NoChildren, PlanDescriptionImpl, renderAsTreeTable}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.{CypherFunSuite, WindowsStringSafe}
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId

class ExecutionPlanDescriptionTest extends CypherFunSuite {
  implicit val windowsSafe = WindowsStringSafe

  test("use variable name instead of ReferenceFromSlot") {

    val arguments = Seq(
      Rows(42),
      DbHits(33),
      Expression(ReferenceFromSlot(42, "  id@23")),
      EstimatedRows(1))

    val plan = PlanDescriptionImpl(LogicalPlanId.DEFAULT, "NAME", NoChildren, arguments, Set("  n@76"))

    val details = renderAsTreeTable(plan)
    details should equal(
      """+----------+----------------+------+---------+-----------+-------+
        || Operator | Estimated Rows | Rows | DB Hits | Variables | Other |
        |+----------+----------------+------+---------+-----------+-------+
        || +NAME    |              1 |   42 |      33 | n         | id    |
        |+----------+----------------+------+---------+-----------+-------+
        |""".stripMargin)
  }

}
